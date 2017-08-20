package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/tsdb/labels"

	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/tsdb"
	"github.com/spf13/pflag"
)

var (
	stepTime = 1 * time.Hour
)

type migrateConfig struct {
	InputDirectory  string
	OutputDirectory string
	RetentionTime   time.Duration
	StartTime       time.Time
	StepTime        time.Duration
}

func parseFlags() (migrateConfig, error) {
	config := migrateConfig{
		InputDirectory:  "",
		OutputDirectory: "",
		RetentionTime:   15 * 24 * time.Hour,
		StartTime:       time.Date(2016, 7, 18, 14, 37, 0, 0, time.UTC),
		StepTime:        stepTime,
	}

	startTimeStr := config.StartTime.Format(time.RFC3339)

	pflag.StringVarP(&config.InputDirectory, "input", "i", config.InputDirectory, "Directory of local storage to convert.")
	pflag.StringVarP(&config.OutputDirectory, "output", "o", config.OutputDirectory, "Directory for new TSDB database.")
	pflag.DurationVarP(&config.RetentionTime, "retention", "r", config.RetentionTime, "Retention time of new database.")
	pflag.StringVarP(&startTimeStr, "start-time", "s", startTimeStr, "Starting time for conversion process.")
	pflag.DurationVar(&config.StepTime, "step-time", config.StepTime, "Time slice to use for copying values.")
	pflag.Parse()

	if err := checkDirectory(config.InputDirectory); err != nil {
		return config, fmt.Errorf("error checking input: %s", err)
	}

	if err := checkDirectory(config.OutputDirectory); err != nil {
		return config, fmt.Errorf("error checking output: %s", err)
	}

	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		return config, fmt.Errorf("error parsing start time: %s", err)
	}
	config.StartTime = startTime

	if config.StepTime < time.Hour {
		return config, fmt.Errorf("step too small (min. 1 hour): %s", config.StepTime)
	}

	return config, nil
}

func checkDirectory(dir string) error {
	if dir == "" {
		pflag.Usage()
		return errors.New("not specified")
	}

	stat, err := os.Stat(dir)
	switch {
	case os.IsNotExist(err):
		return fmt.Errorf("does not exist: %s", dir)
	case err != nil:
		return fmt.Errorf("error getting info for %s: %s", dir, err)
	}

	if !stat.IsDir() {
		return fmt.Errorf("no directory: %s", dir)
	}

	return nil
}

func main() {
	config, err := parseFlags()
	if err != nil {
		log.Fatalf("Error in flags: %s", err)
	}

	storageOpts := &local.MemorySeriesStorageOptions{
		TargetHeapSize:             2 * 1024 * 1024 * 1024,
		PersistenceStoragePath:     config.InputDirectory,
		PersistenceRetentionPeriod: config.RetentionTime,
		HeadChunkTimeout:           5 * time.Minute,
		CheckpointInterval:         24 * time.Hour,
		CheckpointDirtySeriesLimit: 5000,
		Dirty:          false,
		PedanticChecks: false,
		SyncStrategy:   local.Adaptive,
		MinShrinkRatio: 0.1,
		NumMutexes:     4096,
	}

	log.Printf("Opening local storage: %s", config.InputDirectory)
	localStorage := local.NewMemorySeriesStorage(storageOpts)
	if err := localStorage.Start(); err != nil {
		log.Fatalf("Error starting local storage: %s", err)
	}
	defer func() {
		log.Println("Stopping local storage...")
		if err := localStorage.Stop(); err != nil {
			log.Printf("Error stopping local storage: %s", err)
		}
	}()

	tsdbOpts := &tsdb.Options{
		WALFlushInterval:  time.Second * 30,
		RetentionDuration: uint64(config.RetentionTime.Seconds() * 1000),
		BlockRanges:       tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:        false,
	}

	log.Printf("Opening TSDB: %s", config.OutputDirectory)
	db, err := tsdb.Open(config.OutputDirectory, nil, nil, tsdbOpts)
	if err != nil {
		log.Fatalf("Error creating tsdb: %s", err)
	}
	defer func() {
		log.Println("Closing TSDB...")
		db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go runConvert(ctx, done, localStorage, db, config.StartTime, config.StepTime)

	term := make(chan os.Signal)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	select {
	case <-done:
	case <-term:
		log.Printf("Caught interrupt. Exiting...")
	}

	log.Printf("Shutting down...")
	cancel()
}

func runConvert(ctx context.Context, done chan struct{}, input *local.MemorySeriesStorage, output *tsdb.DB, start time.Time, step time.Duration) {
	everything, err := metric.NewLabelMatcher(metric.RegexMatch, "__name__", ".+")
	if err != nil {
		log.Fatalf("Error creating matcher: %s", err)
	}

	now := time.Now()

	timeStamp := start
	for ctx.Err() == nil {
		if now.Before(timeStamp) {
			break
		}

		end := timeStamp.Add(step)

		if err := convertRange(ctx, timeStamp, end, input, output, everything); err != nil {
			log.Fatalf("Error converting range: %s", err)
		}

		timeStamp = end
	}

	done <- struct{}{}
}

func convertRange(ctx context.Context, start, end time.Time, input *local.MemorySeriesStorage, output *tsdb.DB, matcher *metric.LabelMatcher) error {
	modelStart := model.TimeFromUnix(start.Unix())
	modelEnd := model.TimeFromUnix(end.Unix())

	interval := metric.Interval{
		OldestInclusive: modelStart,
		NewestInclusive: modelEnd,
	}

	appender := output.Appender()

	iteratorSlice, err := input.QueryRange(ctx, modelStart, modelEnd, matcher)
	if err != nil {
		return fmt.Errorf("error during query: %s", err)
	}

	metricCount := 0
	sampleCount := 0

	for _, iterator := range iteratorSlice {
		metricCount++

		metric := iterator.Metric().Metric
		labels := convertMetric(metric)

		samples := iterator.RangeValues(interval)
		for _, sample := range samples {
			sampleCount++

			_, err = appender.Add(labels, int64(sample.Timestamp), float64(sample.Value))
			if err != nil {
				log.Printf("Error adding samples: %s", err)
			}
		}
		iterator.Close()
	}

	if err := appender.Commit(); err != nil {
		return fmt.Errorf("error during commit: %s", err)
	}

	log.Printf("TS: %s Metrics: %d Samples: %d", start, metricCount, sampleCount)
	return nil
}

func convertMetric(metric model.Metric) labels.Labels {
	result := make(labels.Labels, 0, len(metric))
	for name, value := range metric {
		result = append(result, labels.Label{
			Name:  string(name),
			Value: string(value),
		})
	}

	sort.Sort(result)
	return result
}

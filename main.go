package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/tsdb"

	"github.com/xperimental/tsdb-migrate/config"
	"github.com/xperimental/tsdb-migrate/minilocal"
)

func main() {
	config, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("Error in flags: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	input, err := runInput(ctx, config.InputDirectory)
	if err != nil {
		log.Fatalf("Error starting input: %s", err)
	}

	output, err := createOutput(config.OutputDirectory, config.RetentionTime)
	if err != nil {
		log.Fatalf("Error creating output: %s", err)
	}

	runLoop(input, output)

	log.Printf("Shutting down...")
	cancel()
}

func runLoop(input <-chan metricSample, output chan<- metricSample) {
	defer close(output)

	term := make(chan os.Signal)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	for {
		select {
		case <-term:
			log.Println("Caught interrupt. Exiting...")

			signal.Reset()
			return
		case in, ok := <-input:
			if !ok {
				log.Println("input closed.")
				return
			}

			output <- in
		}
	}
}

func createOutput(dir string, retentionTime time.Duration) (chan<- metricSample, error) {
	tsdbOpts := &tsdb.Options{
		WALFlushInterval:  5 * time.Minute,
		RetentionDuration: uint64(retentionTime.Seconds() * 1000),
		BlockRanges:       tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:        false,
	}

	log.Printf("Opening TSDB: %s", dir)
	db, err := tsdb.Open(dir, nil, nil, tsdbOpts)
	if err != nil {
		return nil, err
	}

	appender := db.Appender()

	oldest := time.Now().Add(-retentionTime)

	ch := make(chan metricSample)
	go func() {
		for sample := range ch {
			if sample.Time.Time().Before(oldest) {
				continue
			}

			labels := minilocal.ConvertMetric(sample.Metric)

			if _, err := appender.Add(labels, int64(sample.Time), sample.Value); err != nil {
				log.Printf("Error appending value: %s", err)
			}
		}

		if err := appender.Commit(); err != nil {
			log.Printf("Error committing appender: %s", err)
		}

		if err := db.Close(); err != nil {
			log.Printf("Error closing tsdb: %s", err)
		}
	}()

	return ch, nil
}

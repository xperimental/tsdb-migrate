package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/tsdb"

	"github.com/xperimental/tsdb-migrate/config"
	"github.com/xperimental/tsdb-migrate/minilocal"
)

const (
	maxAppendPerAppender = 100000
)

func main() {
	config, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("Error in flags: %s", err)
	}

	input, abortInput, err := runInput(config.InputDirectory)
	if err != nil {
		log.Fatalf("Error starting input: %s", err)
	}

	output, err := createOutput(config.OutputDirectory, config.RetentionTime)
	if err != nil {
		log.Fatalf("Error creating output: %s", err)
	}

	runLoop(input, abortInput, output)

	log.Printf("Shutting down...")
}

func runLoop(input <-chan metricSample, inputAbort chan<- struct{}, output chan<- metricSample) {
	defer close(output)

	term := make(chan os.Signal)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	for {
		select {
		case <-term:
			log.Println("Caught interrupt. Exiting...")
			go func() {
				inputAbort <- struct{}{}
			}()

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

	ch := make(chan metricSample)
	go func() {
		oldest := time.Now().Add(-retentionTime)
		var appender tsdb.Appender
		appendCount := 0

		for sample := range ch {
			if sample.Time.Time().Before(oldest) {
				continue
			}

			labels := minilocal.ConvertMetric(sample.Metric)

			if appender == nil {
				appender = db.Appender()
				appendCount = 0
			}

			log.Printf("append time: %s", time.Unix(int64(sample.Time/1000), 0))
			if _, err := appender.Add(labels, int64(sample.Time), sample.Value); err != nil {
				log.Fatalf("Error appending value %#v: %s", sample, err)
			}
			appendCount++

			if appendCount > maxAppendPerAppender {
				if err := appender.Commit(); err != nil {
					log.Printf("Error committing appender: %s", err)
				}
				appender = nil
			}
		}

		if appender != nil {
			if err := appender.Commit(); err != nil {
				log.Printf("Error committing appender: %s", err)
			}
		}

		if err := db.Close(); err != nil {
			log.Printf("Error closing tsdb: %s", err)
		}
	}()

	return ch, nil
}

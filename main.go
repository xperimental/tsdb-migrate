package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/tsdb"
	"github.com/xperimental/tsdb-migrate/config"
)

func main() {
	config, err := config.ParseFlags()
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
		WALFlushInterval:  5 * time.Minute,
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

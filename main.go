package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/tsdb"
	"github.com/spf13/pflag"
)

type migrateConfig struct {
	RetentionTime   time.Duration
	InputDirectory  string
	OutputDirectory string
}

func parseFlags() (migrateConfig, error) {
	var config migrateConfig
	pflag.StringVarP(&config.InputDirectory, "input", "i", "", "Directory of local storage to convert.")
	pflag.StringVarP(&config.OutputDirectory, "output", "o", "", "Directory for new TSDB database.")
	pflag.DurationVarP(&config.RetentionTime, "retention", "r", 15*24*time.Hour, "Retention time of new database.")
	pflag.Parse()

	if err := checkDirectory(config.InputDirectory); err != nil {
		return config, fmt.Errorf("error checking input: %s", err)
	}

	if err := checkDirectory(config.OutputDirectory); err != nil {
		return config, fmt.Errorf("error checking output: %s", err)
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
}

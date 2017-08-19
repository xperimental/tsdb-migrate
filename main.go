package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

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

	opts := &tsdb.Options{
		WALFlushInterval:  time.Second * 30,
		RetentionDuration: uint64(config.RetentionTime.Seconds() * 1000),
		BlockRanges:       tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:        false,
	}

	log.Printf("Opening TSDB: %s", config.OutputDirectory)
	db, err := tsdb.Open(config.OutputDirectory, nil, nil, opts)
	if err != nil {
		log.Fatalf("Error creating tsdb: %s", err)
	}
	defer func() {
		log.Printf("Closing TSDB...")
		db.Close()
	}()
}

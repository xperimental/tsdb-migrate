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
	OutputDirectory string
}

func parseFlags() (migrateConfig, error) {
	var config migrateConfig
	pflag.StringVarP(&config.OutputDirectory, "output", "o", "", "Directory for new TSDB database.")
	pflag.DurationVarP(&config.RetentionTime, "retention", "r", 15*24*time.Hour, "Retention time of new database.")
	pflag.Parse()

	if config.OutputDirectory == "" {
		pflag.Usage()
		return config, errors.New("need to specify an output directory")
	}

	stat, err := os.Stat(config.OutputDirectory)
	switch {
	case os.IsNotExist(err):
		return config, fmt.Errorf("output does not exist: %s", config.OutputDirectory)
	case err != nil:
		return config, fmt.Errorf("error getting info of output %s: %s", config.OutputDirectory, err)
	}

	if !stat.IsDir() {
		return config, fmt.Errorf("output is no directory: %s", config.OutputDirectory)
	}

	return config, nil
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

	db, err := tsdb.Open(config.OutputDirectory, nil, nil, opts)
	if err != nil {
		log.Fatalf("Error creating tsdb: %s", err)
	}
	defer db.Close()
}

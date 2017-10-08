package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/pflag"
)

// MigrateConfig contains the configuration of the migration tool.
type MigrateConfig struct {
	InputDirectory  string
	OutputDirectory string
	RetentionTime   time.Duration
	FlushInterval   int
	BufferSize      int
}

var defaultConfig = MigrateConfig{
	InputDirectory:  "",
	OutputDirectory: "",
	RetentionTime:   15 * 24 * time.Hour,
	FlushInterval:   10000000,
	BufferSize:      10000,
}

// ParseFlags creates a new configuration from the command-line parameters.
func ParseFlags() (MigrateConfig, error) {
	config := defaultConfig

	pflag.StringVarP(&config.InputDirectory, "input", "i", config.InputDirectory, "Directory of local storage to convert.")
	pflag.StringVarP(&config.OutputDirectory, "output", "o", config.OutputDirectory, "Directory for new TSDB database.")
	pflag.DurationVarP(&config.RetentionTime, "retention", "r", config.RetentionTime, "Retention time of new database.")
	pflag.IntVarP(&config.FlushInterval, "flush-interval", "f", config.FlushInterval, "Flush interval in number of samples.")
	pflag.IntVar(&config.BufferSize, "buffer-size", config.BufferSize, "Input buffer size.")
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

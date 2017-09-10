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
	StartTime       time.Time
	StepTime        time.Duration
}

var defaultConfig = MigrateConfig{
	InputDirectory:  "",
	OutputDirectory: "",
	RetentionTime:   15 * 24 * time.Hour,
	StartTime:       time.Date(2016, 7, 18, 14, 37, 0, 0, time.UTC),
	StepTime:        24 * time.Hour,
}

// ParseFlags creates a new configuration from the command-line parameters.
func ParseFlags() (MigrateConfig, error) {
	config := defaultConfig

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

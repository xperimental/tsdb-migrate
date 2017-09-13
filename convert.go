package main

import (
	"context"
	"fmt"
	"log"

	"github.com/xperimental/tsdb-migrate/minilocal"
)

func runInput(ctx context.Context, inputDir string) (chan string, error) {
	ch := make(chan string)
	go func() {
		defer close(ch)

		seriesMap, err := minilocal.LoadSeriesMap(inputDir)
		if err != nil {
			log.Printf("Error loading series map: %s", err)
			return
		}

		for _, s := range seriesMap {
			fmt.Printf("%s: complete? %v\n", s.Metric(), s.AllInMemory())
		}
	}()
	return ch, nil
}

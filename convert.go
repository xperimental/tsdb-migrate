package main

import (
	"context"
	"fmt"
	"log"

	"github.com/prometheus/common/model"
	"github.com/xperimental/tsdb-migrate/fprgroup"
	"github.com/xperimental/tsdb-migrate/minilocal"
)

type metricSample struct {
	Metric model.Metric
	Time   model.Time
	Value  float64
}

func runInput(ctx context.Context, inputDir string) (chan metricSample, error) {
	ch := make(chan metricSample)
	go func() {
		defer close(ch)

		seriesMap, err := minilocal.LoadSeriesMap(inputDir)
		if err != nil {
			log.Printf("Error loading series map: %s", err)
			return
		}

		groupedByTime := fprgroup.CreateGroups(seriesMap)

		for i, group := range groupedByTime {
			diff := 0
			if i > 0 {
				diff = int(groupedByTime[i-1].To - group.From)
			}
			fmt.Printf("group %d: %d (%d) -> %d; %d metrics\n", i, group.From, diff, group.To, len(group.Fingerprints))
		}
	}()
	return ch, nil
}

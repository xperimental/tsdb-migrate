package main

import (
	"context"
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

		readers := make(map[model.Fingerprint]minilocal.SampleReader)
		for _, group := range groupedByTime {
			for fpr := range group.Fingerprints {
				if _, ok := readers[fpr]; ok {
					continue
				}

				log.Printf("Opening reader for %s...", fpr)
				chunkReader, err := minilocal.NewReader(inputDir, fpr)
				if err != nil {
					log.Printf("Error opening reader for %s: %s", fpr, err)
					continue
				}

				readers[fpr] = minilocal.NewSampleReader(fpr, chunkReader)
			}

			// Read samples from readers
		}
	}()
	return ch, nil
}

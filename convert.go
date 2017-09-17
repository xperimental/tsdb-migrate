package main

import (
	"context"
	"log"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/xperimental/tsdb-migrate/minilocal"
)

type seriesTime struct {
	Fingerprint model.Fingerprint
	From        model.Time
	To          model.Time
}

var timeStep model.Time = 60 * 1000

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

		timeSortedFingerprints := sortedFingerprints(seriesMap)

		for _, seriesInfo := range timeSortedFingerprints {
			series := seriesMap[seriesInfo.Fingerprint]
			metric := series.Metric()

			log.Printf("Series: %s", metric)

			chunks, err := minilocal.ReadChunks(inputDir, seriesInfo.Fingerprint)
			if err != nil {
				log.Printf("Error opening chunk file: %s", err)
				continue
			}

			for _, chunk := range chunks {
				iterator := chunk.NewIterator()

				for iterator.Scan() {
					sample := iterator.Value()
					ch <- metricSample{
						Metric: metric,
						Time:   sample.Timestamp,
						Value:  float64(sample.Value),
					}
				}

				if iterator.Err() != nil {
					log.Printf("Error reading chunk: %s", iterator.Err())
					continue
				}
			}
		}
	}()
	return ch, nil
}

func sortedFingerprints(seriesMap minilocal.SeriesMap) []seriesTime {
	fingerprints := []seriesTime{}
	for fpr, series := range seriesMap {
		fingerprints = append(fingerprints, seriesTime{
			Fingerprint: fpr,
			From:        series.First(),
			To:          series.Last(),
		})
	}

	sort.Slice(fingerprints, func(i, j int) bool {
		itemI := fingerprints[i]
		itemJ := fingerprints[j]

		if itemI.From == itemJ.From {
			return itemI.To < itemJ.To
		}

		return itemI.From < itemJ.From
	})

	return fingerprints
}

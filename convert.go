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

func runInput(ctx context.Context, inputDir string) (chan string, error) {
	ch := make(chan string)
	go func() {
		defer close(ch)

		seriesMap, err := minilocal.LoadSeriesMap(inputDir)
		if err != nil {
			log.Printf("Error loading series map: %s", err)
			return
		}

		timeSortedFingerprints := sortedFingerprints(seriesMap)
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

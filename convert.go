package main

import (
	"context"
	"log"

	"github.com/prometheus/prometheus/storage/local/codable"

	"github.com/prometheus/prometheus/storage/local/index"
)

func runInput(ctx context.Context, inputDir string) (chan string, error) {
	metricsIndex, err := index.NewFingerprintMetricIndex(inputDir)
	if err != nil {
		log.Printf("Error opening index: %s", err)
		return nil, err
	}

	ch := make(chan string)
	go func() {
		defer close(ch)

		var fpr codable.Fingerprint
		var mtr codable.Metric
		metricsIndex.ForEach(func(kv index.KeyValueAccessor) error {
			if err := kv.Key(&fpr); err != nil {
				log.Printf("Error decoding key: %s", err)
				return err
			}

			if err := kv.Value(&mtr); err != nil {
				log.Printf("Error decoding value: %s", err)
				return err
			}

			log.Printf("%d -> %s", fpr, mtr)
			return nil
		})
	}()
	return ch, nil
}

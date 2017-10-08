package main

import (
	"log"
	"sort"
	"time"

	"github.com/prometheus/common/model"
	"github.com/xperimental/tsdb-migrate/minilocal"
)

type metricSample struct {
	Fingerprint model.Fingerprint
	Metric      model.Metric
	Time        model.Time
	Value       float64
}

func runInput(inputDir string, bufferSize int) (chan metricSample, chan struct{}, error) {
	ch := make(chan metricSample, bufferSize)
	abort := make(chan struct{})
	go func() {
		defer close(ch)

		seriesMap, err := minilocal.LoadSeriesMap(inputDir)
		if err != nil {
			log.Printf("Error loading series map: %s", err)
			return
		}

		readers := make(map[model.Fingerprint]minilocal.SampleReader)
		for fpr := range seriesMap {
			if _, ok := readers[fpr]; ok {
				continue
			}

			chunkReader, err := minilocal.NewReader(inputDir, fpr)
			if err != nil {
				log.Printf("Error opening reader for %s: %s", fpr, err)
				continue
			}

			readers[fpr] = minilocal.NewSampleReader(fpr, chunkReader)
		}

		for {
			select {
			case <-abort:
				log.Printf("Aborting input...")
				return
			default:
			}

			if len(readers) == 0 {
				log.Printf("No open readers left. Stopping.")
				return
			}

			samples := make([]metricSample, 0, len(readers))
			toClose := []model.Fingerprint{}
			for fpr, r := range readers {
				sample, err := r.Read()
				switch {
				case err != nil:
					toClose = append(toClose, fpr)
				default:
					samples = append(samples, metricSample{
						Fingerprint: fpr,
						Metric:      seriesMap[sample.Fingerprint].Metric(),
						Time:        sample.Time,
						Value:       sample.Value,
					})
				}
			}

			for _, fpr := range toClose {
				r := readers[fpr]
				delete(readers, fpr)

				if err := r.Close(); err != nil {
					log.Printf("Error closing reader for %s: %s", fpr, err)
				}
			}

			sort.Slice(samples, func(i, j int) bool {
				return samples[i].Time < samples[j].Time
			})

			sum := time.Duration(0)
			count := 0
			for {
				start := time.Now()
				if len(samples) == 0 {
					log.Printf("No samples left.")
					break
				}

				var sample metricSample
				sample, samples = samples[0], samples[1:]

				ch <- sample

				fpr := sample.Fingerprint
				next, err := readers[fpr].Read()
				if err != nil {
					log.Printf("Closing reader for %s", fpr)

					samples = samples[1:]
					r := readers[fpr]
					delete(readers, fpr)

					if err := r.Close(); err != nil {
						log.Printf("Error closing reader for %s: %s", fpr, err)
					}

					continue
				}

				i := 0
				for ; i < len(samples); i++ {
					if samples[i].Time >= next.Time {
						break
					}
				}

				samples = append(samples, metricSample{})
				copy(samples[i+1:], samples[i:])
				samples[i] = metricSample{
					Fingerprint: fpr,
					Metric:      sample.Metric,
					Time:        next.Time,
					Value:       next.Value,
				}

				count++
				sum += time.Now().Sub(start)
			}
		}
	}()
	return ch, abort, nil
}

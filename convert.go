package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

var stepTime = 1 * time.Hour

func runConvert(ctx context.Context, done chan struct{}, input *local.MemorySeriesStorage, output *tsdb.DB, start time.Time, step time.Duration) {
	everything, err := metric.NewLabelMatcher(metric.RegexMatch, "__name__", ".+")
	if err != nil {
		log.Fatalf("Error creating matcher: %s", err)
	}

	now := time.Now()
	fprCache := make(map[string]string)

	timeStamp := start
	for ctx.Err() == nil {
		if now.Before(timeStamp) {
			break
		}

		end := timeStamp.Add(step)

		if err := convertRange(ctx, fprCache, timeStamp, end, input, output, everything); err != nil {
			log.Fatalf("Error converting range: %s", err)
		}

		timeStamp = end
	}

	done <- struct{}{}
}

func convertRange(ctx context.Context, fprCache map[string]string, start, end time.Time, input *local.MemorySeriesStorage, output *tsdb.DB, matcher *metric.LabelMatcher) error {
	modelStart := model.TimeFromUnix(start.Unix())
	modelEnd := model.TimeFromUnix(end.Unix())

	interval := metric.Interval{
		OldestInclusive: modelStart,
		NewestInclusive: modelEnd,
	}

	appender := output.Appender()

	iteratorSlice, err := input.QueryRange(ctx, modelStart, modelEnd, matcher)
	if err != nil {
		return fmt.Errorf("error during query: %s", err)
	}

	metricCount := 0
	sampleCount := 0

	for _, iterator := range iteratorSlice {
		metricCount++

		metric := iterator.Metric().Metric
		labels := convertMetric(metric)
		fpr := labels.String()

		samples := iterator.RangeValues(interval)
		for _, sample := range samples {
			sampleCount++

			ref, ok := fprCache[fpr]
			if ok {
				switch err := appender.AddFast(ref, int64(sample.Timestamp), float64(sample.Value)); err {
				case nil:
				case tsdb.ErrNotFound:
					ok = false
					log.Printf("Ref not found: %s", fpr)
				case tsdb.ErrOutOfOrderSample, tsdb.ErrOutOfBounds:
					log.Printf("Non-fatal error during append: %s", err)
					continue
				default:
					return fmt.Errorf("Error adding samples by ref: %s", err)
				}
			}

			if !ok {
				ref, err = appender.Add(labels, int64(sample.Timestamp), float64(sample.Value))
				switch err {
				case nil:
				case tsdb.ErrOutOfOrderSample, tsdb.ErrOutOfBounds:
					log.Printf("Non-fatal error during append: %s", err)
					continue
				default:
					return fmt.Errorf("Error adding samples: %s", err)
				}
				if len(ref) > 0 {
					fprCache[fpr] = ref
				}
			}
		}
		iterator.Close()
	}

	if err := appender.Commit(); err != nil {
		return fmt.Errorf("error during commit: %s", err)
	}

	log.Printf("TS: %s Metrics: %d Samples: %d", start, metricCount, sampleCount)
	return nil
}

func convertMetric(metric model.Metric) labels.Labels {
	result := make(labels.Labels, 0, len(metric))
	for name, value := range metric {
		result = append(result, labels.Label{
			Name:  string(name),
			Value: string(value),
		})
	}

	sort.Sort(result)
	return result
}

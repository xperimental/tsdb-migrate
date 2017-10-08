package minilocal

import (
	"log"
	"path/filepath"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/tsdb/labels"
)

// SeriesMap maps a fingerprint to a timeseries.
type SeriesMap map[model.Fingerprint]Series

// LoadSeriesMap loads the series from the heads file.
func LoadSeriesMap(inputDir string) (SeriesMap, error) {
	seriesMap := make(SeriesMap)

	log.Printf("Loading series...")
	headsFile := filepath.Join(inputDir, headsFileName)
	hs := newHeadsScanner(headsFile)

	heads := make(map[model.Fingerprint]interface{})
	for hs.scan() {
		seriesMap[hs.fp] = hs.series
	}

	if hs.err != nil {
		return nil, hs.err
	}

	log.Printf("%d series loaded.", len(heads))
	return seriesMap, nil
}

var labelsCache = make(map[model.Fingerprint]labels.Labels)

func ConvertMetric(fpr model.Fingerprint, metric model.Metric) labels.Labels {
	if labels, ok := labelsCache[fpr]; ok {
		return labels
	}

	result := make(labels.Labels, 0, len(metric))
	for name, value := range metric {
		result = append(result, labels.Label{
			Name:  string(name),
			Value: string(value),
		})
	}

	sort.Sort(result)

	labelsCache[fpr] = result
	return result
}

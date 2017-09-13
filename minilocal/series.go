package minilocal

import (
	"log"
	"path/filepath"

	"github.com/prometheus/common/model"
)

type SeriesMap map[model.Fingerprint]Series

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

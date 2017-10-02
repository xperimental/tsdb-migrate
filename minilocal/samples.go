package minilocal

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

// Sample contains one sample of one timeseries at a certain time.
type Sample struct {
	Time  model.Time
	Value float64
}

// SampleReader provides an interface to read samples from a metric in time order.
type SampleReader interface {
	Read() (Sample, error)
	Close() error
}

type sampleReader struct {
	reader   ChunkReadCloser
	iterator chunk.Iterator
}

func (r *sampleReader) Read() (Sample, error) {
	success := false
	for !success {
		if r.iterator == nil {
			chunk, err := r.reader.Read()
			if err != nil {
				return Sample{}, err
			}

			r.iterator = chunk.NewIterator()
		}

		success = r.iterator.Scan()
		if !success {
			err := r.iterator.Err()
			if err != nil {
				return Sample{}, err
			}

			r.iterator = nil
		}
	}

	pair := r.iterator.Value()
	return Sample{
		Time:  pair.Timestamp,
		Value: float64(pair.Value),
	}, nil
}

func (r *sampleReader) Close() error {
	r.iterator = nil
	return r.reader.Close()
}

// NewSampleReader creates a new SampleReader.
func NewSampleReader(chunkReader ChunkReadCloser) SampleReader {
	return &sampleReader{
		reader:   chunkReader,
		iterator: nil,
	}
}

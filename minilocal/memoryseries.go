package minilocal

import (
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

type memorySeries struct {
	metric model.Metric
	// Sorted by start time, overlapping chunk ranges are forbidden.
	chunkDescs []*chunk.Desc
	// The index (within chunkDescs above) of the first chunk.Desc that
	// points to a non-persisted chunk. If all chunks are persisted, then
	// persistWatermark == len(chunkDescs).
	persistWatermark int
	// The modification time of the series file. The zero value of time.Time
	// is used to mark an unknown modification time.
	modTime time.Time
	// The chunkDescs in memory might not have all the chunkDescs for the
	// chunks that are persisted to disk. The missing chunkDescs are all
	// contiguous and at the tail end. chunkDescsOffset is the index of the
	// chunk on disk that corresponds to the first chunk.Desc in memory. If
	// it is 0, the chunkDescs are all loaded. A value of -1 denotes a
	// special case: There are chunks on disk, but the offset to the
	// chunkDescs in memory is unknown. Also, in this special case, there is
	// no overlap between chunks on disk and chunks in memory (implying that
	// upon first persisting of a chunk in memory, the offset has to be
	// set).
	chunkDescsOffset int
	// The savedFirstTime field is used as a fallback when the
	// chunkDescsOffset is not 0. It can be used to save the FirstTime of the
	// first chunk before its chunk desc is evicted. In doubt, this field is
	// just set to the oldest possible timestamp.
	savedFirstTime model.Time
	// The timestamp of the last sample in this series. Needed for fast
	// access for federation and to ensure timestamp monotonicity during
	// ingestion.
	lastTime model.Time
	// The last ingested sample value. Needed for fast access for
	// federation.
	lastSampleValue model.SampleValue
	// Whether lastSampleValue has been set already.
	lastSampleValueSet bool
	// Whether the current head chunk has already been finished.  If true,
	// the current head chunk must not be modified anymore.
	headChunkClosed bool
	// Whether the current head chunk is used by an iterator. In that case,
	// a non-closed head chunk has to be cloned before more samples are
	// appended.
	headChunkUsedByIterator bool
	// Whether the series is inconsistent with the last checkpoint in a way
	// that would require a disk seek during crash recovery.
	dirty bool
}

type Series interface {
	Metric() model.Metric
	Descs() []*chunk.Desc
	AllInMemory() bool
}

func (m *memorySeries) Metric() model.Metric {
	return m.metric
}

func (m *memorySeries) Descs() []*chunk.Desc {
	return m.chunkDescs
}

func (m *memorySeries) AllInMemory() bool {
	return m.chunkDescsOffset == 0
}

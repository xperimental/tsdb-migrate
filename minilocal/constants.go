package minilocal

import (
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

const (
	// Version of the storage as it can be found in the version file.
	// Increment to protect against incompatible changes.
	Version         = 1
	versionFileName = "VERSION"

	seriesFileSuffix     = ".db"
	seriesTempFileSuffix = ".db.tmp"
	seriesDirNameLen     = 2 // How many bytes of the fingerprint in dir name.
	hintFileSuffix       = ".hint"

	mappingsFileName      = "mappings.db"
	mappingsTempFileName  = "mappings.db.tmp"
	mappingsFormatVersion = 1
	mappingsMagicString   = "PrometheusMappings"

	dirtyFileName = "DIRTY"

	fileBufSize = 1 << 16 // 64kiB.

	chunkHeaderLen             = 17
	chunkHeaderTypeOffset      = 0
	chunkHeaderFirstTimeOffset = 1
	chunkHeaderLastTimeOffset  = 9
	chunkLenWithHeader         = chunk.ChunkLen + chunkHeaderLen
	chunkMaxBatchSize          = 62 // Max no. of chunks to load or write in
	// one batch.  Note that 62 is the largest number of chunks that fit
	// into 64kiB on disk because chunkHeaderLen is added to each 1k chunk.

	indexingMaxBatchSize  = 1024 * 1024
	indexingBatchTimeout  = 500 * time.Millisecond // Commit batch when idle for that long.
	indexingQueueCapacity = 1024 * 256
)

var fpLen = len(model.Fingerprint(0).String()) // Length of a fingerprint as string.

const (
	flagHeadChunkPersisted byte = 1 << iota
	// Add more flags here like:
	// flagFoo
	// flagBar
)

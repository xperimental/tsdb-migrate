package minilocal

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

var (
	// ErrAlreadyClosed is used when reading from an already closed reader.
	ErrAlreadyClosed = errors.New("already closed")
)

// ChunkReadCloser provides an interface to read chunks from a series.
type ChunkReadCloser interface {
	Read() (chunk.Chunk, error)
	Close() error
}

type chunkReader struct {
	fileName string
	chunks   int
	current  int
	closed   bool
}

func (r *chunkReader) Read() (chunk.Chunk, error) {
	if r.closed {
		return nil, ErrAlreadyClosed
	}

	if r.current == r.chunks {
		return nil, io.EOF
	}

	file, err := os.Open(r.fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	pos := int64(r.current * chunkLenWithHeader)
	if _, err := file.Seek(pos, 0); err != nil {
		return nil, err
	}

	r.current++
	chunk, err := readChunk(file)
	if err != nil {
		return nil, err
	}

	return chunk, nil
}

func (r *chunkReader) Close() error {
	r.closed = true
	r.chunks = 0
	r.current = 0

	return nil
}

// NewReader creates a new reader to read chunks from a series.
func NewReader(baseDir string, fpr model.Fingerprint) (ChunkReadCloser, error) {
	fileName := fileNameForFingerprint(baseDir, fpr)

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	chunkCount := int(fileSize / chunkLenWithHeader)

	return &chunkReader{
		fileName: fileName,
		chunks:   chunkCount,
		current:  0,
		closed:   false,
	}, nil
}

// ReadChunks reads all chunks for a fingerprint.
func ReadChunks(baseDir string, fpr model.Fingerprint) ([]chunk.Chunk, error) {
	reader, err := NewReader(baseDir, fpr)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	chunks := []chunk.Chunk{}
	for {
		chunk, err := reader.Read()
		switch {
		case err == io.EOF:
			return chunks, nil
		case err != nil:
			return nil, err
		default:
			chunks = append(chunks, chunk)
		}
	}
}

func readChunk(reader io.Reader) (chunk.Chunk, error) {
	buf := make([]byte, chunkLenWithHeader)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}

	chunk, err := chunk.NewForEncoding(chunk.Encoding(buf[chunkHeaderTypeOffset]))
	if err != nil {
		return nil, err
	}
	if err := chunk.UnmarshalFromBuf(buf[chunkHeaderLen:]); err != nil {
		return nil, err
	}

	return chunk, nil
}

func fileNameForFingerprint(basePath string, fp model.Fingerprint) string {
	fpStr := fp.String()
	return filepath.Join(basePath, fpStr[0:seriesDirNameLen], fpStr[seriesDirNameLen:]+seriesFileSuffix)
}

package minilocal

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

// ChunkReadCloser provides an interface to read chunks from a series.
type ChunkReadCloser interface {
	Read() (chunk.Chunk, error)
	Close() error
}

type chunkReader struct {
	file    *os.File
	chunks  int
	current int
}

func (r *chunkReader) Read() (chunk.Chunk, error) {
	if r.current == r.chunks {
		return nil, io.EOF
	}

	r.current++
	chunk, err := readChunk(r.file)
	if err != nil {
		return nil, err
	}

	return chunk, nil
}

func (r *chunkReader) Close() error {
	r.chunks = 0
	r.current = 0

	return r.file.Close()
}

// NewReader creates a new reader to read chunks from a series.
func NewReader(baseDir string, fpr model.Fingerprint) (ChunkReadCloser, error) {
	fileName := fileNameForFingerprint(baseDir, fpr)

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	chunkCount := int(fileSize / chunkLenWithHeader)
	log.Printf("file size: %d chunks: %d", fileSize, chunkCount)

	return &chunkReader{
		file:    file,
		chunks:  chunkCount,
		current: -1,
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

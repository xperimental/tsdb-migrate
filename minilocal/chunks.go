package minilocal

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

// ReadChunks reads all chunks for a fingerprint.
func ReadChunks(baseDir string, fpr model.Fingerprint) ([]chunk.Chunk, error) {
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
	log.Printf("file size: %d chunks: %d", fileSize, chunkCount)

	chunks := make([]chunk.Chunk, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		buf := make([]byte, chunkLenWithHeader)
		if _, err := io.ReadFull(file, buf); err != nil {
			return nil, err
		}

		chunk, err := chunk.NewForEncoding(chunk.Encoding(buf[chunkHeaderTypeOffset]))
		if err != nil {
			return nil, err
		}
		if err := chunk.UnmarshalFromBuf(buf[chunkHeaderLen:]); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func fileNameForFingerprint(basePath string, fp model.Fingerprint) string {
	fpStr := fp.String()
	return filepath.Join(basePath, fpStr[0:seriesDirNameLen], fpStr[seriesDirNameLen:]+seriesFileSuffix)
}

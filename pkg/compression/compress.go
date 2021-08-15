package compression

import (
	"d7y.io/dragonfly/v2/pkg/compression/gzip"
	"io"
)

type CompressAlgorithm string

const (
	GZIP CompressAlgorithm = "gzip"
)

func NewCompress(algorithm CompressAlgorithm) Compress {
	switch algorithm {
	case GZIP:
		return gzip.NewGzipCompress()
	}
	return nil
}

type Compress interface {

	// UnCompression decode compression data
	UnCompression(reader io.Reader) (io.ReadCloser, error)

	// Compression compression data
	Compression(writer io.Writer) (io.WriteCloser, error)

	// CompressRatio return compression ratio
	CompressRatio(data []byte) (ratio float32, err error)
}

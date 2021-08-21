package compression

import (
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/compression/gzip"
	"github.com/pkg/errors"
	"io"
)

type CompressAlgorithm string

const (
	GZIP CompressAlgorithm = "gzip"

	ZSTD CompressAlgorithm = "zstd"
)

func (ca CompressAlgorithm) String() string {
	return string(ca)
}

func CompressAlgorithmMapPieceStyle(algorithm CompressAlgorithm) (types.PieceFormat, error) {
	switch algorithm {
	case GZIP:
		return types.GzipCompressAlgorithm, nil
	default:
		return types.PlainUnspecified, errors.Errorf("not support current algorithm %s", algorithm.String())
	}
}

func PieceStyleMapCompressAlgorithm(format types.PieceFormat) CompressAlgorithm {
	switch format {
	case types.GzipCompressAlgorithm:
		return GZIP
	default:
		return ""
	}
}

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

package gzip

import (
	"bytes"
	"compress/gzip"
	"github.com/pkg/errors"
	"io"
	"sync"
)

const (
	// SpeedAndRatioLB Set the compression speed and compression ratio equalization level
	SpeedAndRatioLB = 4
)

type gzipCompress struct {
	bufPool *sync.Pool
}

func NewGzipCompress() *gzipCompress {
	var bufPool = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	return &gzipCompress{
		bufPool: bufPool,
	}
}

// UnCompression un compression data
func (gc *gzipCompress) UnCompression(reader io.Reader) (io.ReadCloser, error) {
	r, err := gzip.NewReader(reader)
	return r, err
}

// Compression compression data
func (gc *gzipCompress) Compression(writer io.Writer) (io.WriteCloser, error) {
	w, err := gzip.NewWriterLevel(writer, SpeedAndRatioLB)
	return w, err
}

// CompressRatio return compression ratio
func (gc *gzipCompress) CompressRatio(data []byte) (ratio float32, err error) {
	var bb = gc.bufPool.Get().(*bytes.Buffer)
	bb.Reset()
	w, err := gzip.NewWriterLevel(bb, SpeedAndRatioLB)
	if err != nil {
		return -1, errors.Wrap(err, "compression: set gzip compress level error")
	}
	written, err := w.Write(data)
	if err != nil {
		return -1, errors.Wrap(err, "compression: gzip write data error")
	}
	err = w.Close()
	if err != nil {
		return -1, errors.Wrap(err, "compression: gzip write close error")
	}
	return float32(float64(written) / float64(bb.Len())), nil
}

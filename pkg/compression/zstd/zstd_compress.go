package zstd

import (
	"github.com/klauspost/compress/zstd"
	"io"
)

type zstdCompress struct {
	zstd.Decoder
	zstd.Encoder
}

// UnCompression decode compression data
func (zc *zstdCompress) UnCompression(in io.Reader) (io.ReadCloser, error) {
	_, err := zstd.NewReader(in)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (zc *zstdCompress) Close() error {
	zc.Decoder.Close()
	return nil
}

// Compression compression data
func (zc zstdCompress) Compression(out io.Writer) (io.WriteCloser, error) {
	enc, err := zstd.NewWriter(out)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

// CompressRatio return compression ratio
func (zc zstdCompress) CompressRatio(data []byte) (ratio float32, err error) {
	return 0, nil
}

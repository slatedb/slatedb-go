package compress

import (
	"bytes"
	"compress/zlib"
	"errors"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/slatedb/slatedb-go/internal/flatbuf"
	"io"
)

const (
	CodecNone Codec = iota
	CodecSnappy
	CodecZlib
	CodecLz4
	CodecZstd
)

type Codec int8

// String converts Codec to string
func (c Codec) String() string {
	switch c {
	case CodecNone:
		return "None"
	case CodecSnappy:
		return "Snappy"
	case CodecZlib:
		return "Zlib"
	case CodecLz4:
		return "LZ4"
	case CodecZstd:
		return "Zstd"
	default:
		return "Unknown"
	}
}

func CodecFromFlatBuf(f flatbuf.CompressionCodec) Codec {
	switch f {
	case flatbuf.CompressionCodecNone:
		return CodecNone
	case flatbuf.CompressionCodecSnappy:
		return CodecSnappy
	case flatbuf.CompressionCodecZlib:
		return CodecZlib
	case flatbuf.CompressionCodecLz4:
		return CodecLz4
	case flatbuf.CompressionCodecZstd:
		return CodecZstd
	default:
		panic(ErrInvalidCodec.Error())
	}
}

func CodecToFlatBuf(c Codec) flatbuf.CompressionCodec {
	switch c {
	case CodecNone:
		return flatbuf.CompressionCodecNone
	case CodecSnappy:
		return flatbuf.CompressionCodecSnappy
	case CodecZlib:
		return flatbuf.CompressionCodecZlib
	case CodecLz4:
		return flatbuf.CompressionCodecLz4
	case CodecZstd:
		return flatbuf.CompressionCodecZstd
	default:
		panic(ErrInvalidCodec.Error())
	}
}

var ErrInvalidCodec = errors.New("invalid compression codec")

// Encode the provided byte slice
func Encode(buf []byte, codec Codec) ([]byte, error) {
	switch codec {
	case CodecNone:
		return buf, nil

	case CodecSnappy:
		return snappy.Encode(nil, buf), nil

	case CodecZlib:
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		_, err := w.Write(buf)
		_ = w.Close()
		if err != nil {
			return nil, err
		}
		return b.Bytes(), nil

	case CodecLz4:
		var b bytes.Buffer
		w := lz4.NewWriter(&b)
		_, err := w.Write(buf)
		_ = w.Close()
		if err != nil {
			return nil, err
		}
		return b.Bytes(), nil

	case CodecZstd:
		var b bytes.Buffer
		w, err := zstd.NewWriter(&b)
		if err != nil {
			return nil, err
		}
		_, err = w.Write(buf)
		_ = w.Close()
		if err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	default:
		return nil, ErrInvalidCodec
	}
}

// Decode the provided byte slice according to the compression codec
func Decode(buf []byte, codec Codec) ([]byte, error) {
	switch codec {
	case CodecNone:
		return buf, nil

	case CodecSnappy:
		return snappy.Decode(nil, buf)

	case CodecZlib:
		r, err := zlib.NewReader(bytes.NewReader(buf))
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.Close() }()
		return io.ReadAll(r)

	case CodecLz4:
		r := lz4.NewReader(bytes.NewReader(buf))
		return io.ReadAll(r)

	case CodecZstd:
		r, err := zstd.NewReader(bytes.NewReader(buf))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)

	default:
		return nil, ErrInvalidCodec
	}
}

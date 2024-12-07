package compress

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressDecompress(t *testing.T) {
	testCases := []struct {
		name   string
		codec  Codec
		input  []byte
		errMsg string
	}{
		{"None", CodecNone, []byte("Hello, World!"), ""},
		{"Snappy", CodecSnappy, []byte("Snappy compression test"), ""},
		{"Zlib", CodecZlib, []byte("Zlib compression test"), ""},
		{"LZ4", CodecLz4, []byte("LZ4 compression test"), ""},
		{"Zstd", CodecZstd, []byte("Zstd compression test"), ""},
		{"Invalid Codec", Codec(99), []byte("Invalid"), "invalid compression codec"},
		{"Empty Input", CodecSnappy, nil, ""},
		{"Large Input", CodecZstd, bytes.Repeat([]byte("Large input test "), 1000), ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode
			compressed, err := Encode(tc.input, tc.codec)
			if tc.errMsg != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)

			// Decompress
			decompressed, err := Decode(compressed, tc.codec)
			require.NoError(t, err)

			// Compare
			assert.Equal(t, tc.input, decompressed)
		})
	}
}

func TestCompressDecompressNonMatchingCodecs(t *testing.T) {
	input := []byte("Mismatched codec test")
	compressed, err := Encode(input, CodecSnappy)
	require.NoError(t, err)

	_, err = Decode(compressed, CodecZlib)
	assert.Error(t, err)
}

func TestDecompressInvalidInput(t *testing.T) {
	invalidInput := []byte("This is not compressed data")
	codecs := []Codec{CodecSnappy, CodecZlib, CodecLz4, CodecZstd}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			_, err := Decode(invalidInput, codec)
			assert.Error(t, err)
		})
	}
}

func TestCompressDecompressLargeInput(t *testing.T) {
	largeInput := bytes.Repeat([]byte("Large input test "), 100000) // 1.6MB of data
	codecs := []Codec{CodecNone, CodecSnappy, CodecZlib, CodecLz4, CodecZstd}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			compressed, err := Encode(largeInput, codec)
			require.NoError(t, err)

			decompressed, err := Decode(compressed, codec)
			require.NoError(t, err)

			assert.Equal(t, largeInput, decompressed)
		})
	}
}

package sstable_test

import (
	"bytes"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInfoClone(t *testing.T) {
	original := &sstable.Info{
		FirstKey:         []byte("testkey"),
		IndexOffset:      100,
		IndexLen:         200,
		FilterOffset:     300,
		FilterLen:        400,
		CompressionCodec: compress.CodecSnappy,
	}

	clone := original.Clone()

	assert.Equal(t, original.FirstKey, clone.FirstKey)
	assert.Equal(t, original.IndexOffset, clone.IndexOffset)
	assert.Equal(t, original.IndexLen, clone.IndexLen)
	assert.Equal(t, original.FilterOffset, clone.FilterOffset)
	assert.Equal(t, original.FilterLen, clone.FilterLen)
	assert.Equal(t, original.CompressionCodec, clone.CompressionCodec)

	// Ensure that modifying the clone doesn't affect the original
	clone.FirstKey[0] = 'X'
	assert.NotEqual(t, original.FirstKey, clone.FirstKey)
}

func TestInfoEncode(t *testing.T) {
	info := &sstable.Info{
		FirstKey:         []byte("testkey"),
		IndexOffset:      100,
		IndexLen:         200,
		FilterOffset:     300,
		FilterLen:        400,
		CompressionCodec: compress.CodecSnappy,
	}

	codec := &sstable.FlatBufferSSTableInfoCodec{}
	var buf []byte
	info.Encode(&buf, codec)

	assert.NotEmpty(t, buf)
	assert.Greater(t, len(buf), 4) // At least 4 bytes for CRC

	// Decode and verify
	decodedInfo := codec.Decode(buf[:len(buf)-4]) // Exclude CRC
	assert.Equal(t, info.FirstKey, decodedInfo.FirstKey)
	assert.Equal(t, info.IndexOffset, decodedInfo.IndexOffset)
	assert.Equal(t, info.IndexLen, decodedInfo.IndexLen)
	assert.Equal(t, info.FilterOffset, decodedInfo.FilterOffset)
	assert.Equal(t, info.FilterLen, decodedInfo.FilterLen)
	assert.Equal(t, info.CompressionCodec, decodedInfo.CompressionCodec)
}

func TestSsTableInfoCodec(t *testing.T) {
	codec := &sstable.FlatBufferSSTableInfoCodec{}
	
	original := &sstable.Info{
		FirstKey:         []byte("testkey"),
		IndexOffset:      100,
		IndexLen:         200,
		FilterOffset:     300,
		FilterLen:        400,
		CompressionCodec: compress.CodecSnappy,
	}

	encoded := codec.Encode(original)
	decoded := codec.Decode(encoded)

	assert.Equal(t, original.FirstKey, decoded.FirstKey)
	assert.Equal(t, original.IndexOffset, decoded.IndexOffset)
	assert.Equal(t, original.IndexLen, decoded.IndexLen)
	assert.Equal(t, original.FilterOffset, decoded.FilterOffset)
	assert.Equal(t, original.FilterLen, decoded.FilterLen)
	assert.Equal(t, original.CompressionCodec, decoded.CompressionCodec)
}

func TestEncodeTable(t *testing.T) {
	builder := sstable.NewBuilder(20, 10, &sstable.FlatBufferSSTableInfoCodec{}, 10, compress.CodecNone)

	assert.NoError(t, builder.Add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.NoError(t, builder.Add([]byte("key2"), mo.Some([]byte("value2"))))

	table, err := builder.Build()
	assert.NoError(t, err)

	encoded := sstable.EncodeTable(table)

	// Basic checks
	assert.NotEmpty(t, encoded)
	assert.True(t, len(encoded) > len(table.Blocks.At(0)))

	// Check if the encoded table starts with the first block
	assert.True(t, bytes.HasPrefix(encoded, table.Blocks.At(0)))
}

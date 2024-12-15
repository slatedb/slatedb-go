package sstable_test

import (
	"fmt"
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBuilder(t *testing.T) {
	t.Run("Basic Build", func(t *testing.T) {
		builder := sstable.NewBuilder(sstable.Config{
			BlockSize:        4096,
			MinFilterKeys:    10,
			FilterBitsPerKey: 10,
			Compression:      compress.CodecNone,
		})

		// Add some key-value pairs
		require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
		require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))
		require.NoError(t, builder.AddValue([]byte("key3"), []byte("value3")))

		// Build the SSTable
		table, err := builder.Build()
		require.NoError(t, err)
		require.NotNil(t, table)

		// Check table properties
		assert.Equal(t, []byte("key1"), table.Info.FirstKey)
		assert.True(t, table.Bloom.IsAbsent()) // Bloom filter should not be present (less than MinFilterKeys)
		assert.Equal(t, 1, table.Blocks.Len()) // All keys should fit in one block

		// Encode the table
		encoded := sstable.EncodeTable(table)
		assert.NotEmpty(t, encoded)
	})

	t.Run("Multiple Blocks", func(t *testing.T) {

		var blocks [][]types.KeyValue
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			blocks = append(blocks, []types.KeyValue{{Key: key, Value: value}})
		}

		builder := sstable.NewBuilder(sstable.Config{
			BlockSize:        block.V0EstimateBlockSize(blocks[0]), // Small block size to force multiple blocks
			MinFilterKeys:    5,
			FilterBitsPerKey: 10,
			Compression:      compress.CodecNone,
		})

		for _, b := range blocks {
			require.NoError(t, builder.AddValue(b[0].Key, b[0].Value))
		}

		table, err := builder.Build()
		require.NoError(t, err)
		require.NotNil(t, table)

		assert.True(t, table.Blocks.Len() > 1, "Expected multiple blocks")
		assert.True(t, table.Bloom.IsPresent(), "Expected Bloom filter to be present")
	})

	t.Run("Compression", func(t *testing.T) {
		builder := sstable.NewBuilder(sstable.Config{
			BlockSize:        4096,
			MinFilterKeys:    5,
			FilterBitsPerKey: 10,
			Compression:      compress.CodecSnappy,
		})

		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			require.NoError(t, builder.AddValue(key, value))
		}

		table, err := builder.Build()
		require.NoError(t, err)
		require.NotNil(t, table)

		assert.Equal(t, compress.CodecSnappy, table.Info.CompressionCodec)
	})
}

func TestEncodeDecode(t *testing.T) {

	input := [][]types.KeyValue{
		{types.KeyValue{Key: []byte("key1"), Value: []byte("value1")}},
		{types.KeyValue{Key: []byte("key2"), Value: []byte("value2")}},
		{types.KeyValue{Key: []byte("key3"), Value: []byte("value3")}},
	}

	builder := sstable.NewBuilder(sstable.Config{
		BlockSize:        block.V0EstimateBlockSize(input[0]),
		MinFilterKeys:    0,
		FilterBitsPerKey: 10,
		Compression:      compress.CodecNone,
	})

	// Add some key-value pairs
	for _, b := range input {
		require.NoError(t, builder.AddValue(b[0].Key, b[0].Value))
	}

	// Build the SSTable
	table, err := builder.Build()
	assert.NoError(t, err)

	// EncodeTable the table into a []byte
	encoded := sstable.EncodeTable(table)

	assert.NotEmpty(t, encoded)
	blob := sstable.NewBytesBlob(encoded)

	// Decode the Info from the table
	info, err := sstable.ReadInfo(blob)
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, table.Info.FirstKey, info.FirstKey)
	assert.Equal(t, table.Info.IndexOffset, info.IndexOffset)
	assert.Equal(t, table.Info.IndexLen, info.IndexLen)

	// Decode the index from the table
	index, err := sstable.ReadIndex(info, blob)
	assert.NoError(t, err)
	assert.NotNil(t, index)

	// Read the first block from the table
	blocks, err := sstable.ReadBlocks(info, index, common.Range{Start: 0, End: 3}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(input))

	// Should be 1 key per block
	it := block.NewIterator(&blocks[0])
	assert2.NextEntry(t, it, []byte("key1"), []byte("value1"))

	it = block.NewIterator(&blocks[1])
	assert2.NextEntry(t, it, []byte("key2"), []byte("value2"))

	it = block.NewIterator(&blocks[2])
	assert2.NextEntry(t, it, []byte("key3"), []byte("value3"))

	// Test bloom filter
	filter, err := sstable.ReadFilter(info, blob)
	assert.NoError(t, err)
	assert.True(t, filter.IsPresent())
	f, ok := filter.Get()
	assert.True(t, ok)
	assert.True(t, f.HasKey([]byte("key1")))
	assert.True(t, f.HasKey([]byte("key2")))
	assert.True(t, f.HasKey([]byte("key3")))
}

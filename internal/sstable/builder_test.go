package sstable

import (
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncode(t *testing.T) {
	builder := NewBuilder(20, 10, &FlatBufferSSTableInfoCodec{}, 10, compress.CodecNone)

	// Add some key-value pairs
	assert.NoError(t, builder.Add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.NoError(t, builder.Add([]byte("key2"), mo.Some([]byte("value2"))))
	assert.NoError(t, builder.Add([]byte("key3"), mo.Some([]byte("value3"))))

	// Build the SSTable
	table, err := builder.Build()
	assert.NoError(t, err)

	// EncodeTable the table
	encoded := EncodeTable(table)

	// Check that the encoded data is not empty
	assert.NotEmpty(t, encoded)

	// Check that the encoded data length matches the sum of all block lengths
	expectedLength := 0
	for i := 0; i < table.Blocks.Len(); i++ {
		expectedLength += len(table.Blocks.At(i))
	}
	assert.Equal(t, expectedLength, len(encoded))

	// You might want to add more specific checks here, depending on the
	// expected structure of your encoded SSTable
}

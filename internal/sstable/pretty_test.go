package sstable

import (
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrettyPrint(t *testing.T) {
	conf := Config{
		Compression:      compress.CodecNone,
		BlockSize:        45,
		MinFilterKeys:    0, // Force bloom filter
		FilterBitsPerKey: 10,
	}

	builder := NewBuilder(conf)

	// First block
	assert.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
	assert.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))

	// Second block
	assert.NoError(t, builder.AddValue([]byte("key3"), []byte("value3")))
	assert.NoError(t, builder.AddValue([]byte("key4"), []byte("value4")))

	table, err := builder.Build()
	assert.NoError(t, err)

	prettyOutput := PrettyPrint(table)
	//t.Logf("%s", prettyOutput)

	// Add some basic assertions
	assert.Contains(t, prettyOutput, "SSTable Info:")
	assert.Contains(t, prettyOutput, "Blocks:")
	assert.Contains(t, prettyOutput, "key1")
	assert.Contains(t, prettyOutput, "key2")
	assert.Contains(t, prettyOutput, "key3")
}

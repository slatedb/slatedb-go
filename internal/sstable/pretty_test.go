package sstable

import (
	"github.com/samber/mo"
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
	assert.NoError(t, builder.Add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.NoError(t, builder.Add([]byte("key2"), mo.Some([]byte("value2"))))

	// Second block
	assert.NoError(t, builder.Add([]byte("key3"), mo.Some([]byte("value3"))))
	assert.NoError(t, builder.Add([]byte("key4"), mo.Some([]byte("value4"))))

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

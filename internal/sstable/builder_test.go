package sstable

import (
	"fmt"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/stretchr/testify/assert"
	"testing"
)

//func TestBuilderOffsets(t *testing.T) {
//	builder := NewBuilder(20, 10, &FlatBufferSSTableInfoCodec{}, 10, compress.CodecNone)
//
//	// First block
//	assert.NoError(t, builder.Add([]byte("key1"), mo.Some([]byte("value1"))))
//	assert.NoError(t, builder.Add([]byte("key2"), mo.Some([]byte("value2"))))
//
//	// Second block
//	assert.NoError(t, builder.Add([]byte("key3"), mo.Some([]byte("value3"))))
//	assert.NoError(t, builder.Add([]byte("key4"), mo.Some([]byte("value4"))))
//
//	// Build the SSTable
//	table, err := builder.Build()
//	assert.NoError(t, err)
//
//	spew.Dump(table.Info)
//	spew.Dump(table.Bloom)
//}

func TestPrettyPrint(t *testing.T) {
	builder := NewBuilder(20, 10, &FlatBufferSSTableInfoCodec{}, 10, compress.CodecNone)

	// First block
	assert.NoError(t, builder.Add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.NoError(t, builder.Add([]byte("key2"), mo.Some([]byte("value2"))))

	// Second block
	assert.NoError(t, builder.Add([]byte("key3"), mo.Some([]byte("value3"))))
	assert.NoError(t, builder.Add([]byte("key4"), mo.Some([]byte("value4"))))

	table, err := builder.Build()
	assert.NoError(t, err)

	prettyOutput := PrettyPrint(table)
	fmt.Println(prettyOutput)
	//t.Logf("Pretty-printed SSTable:\n%s", prettyOutput)

	// Add some basic assertions
	assert.Contains(t, prettyOutput, "SSTable Info:")
	assert.Contains(t, prettyOutput, "Blocks:")
	//assert.Contains(t, prettyOutput, "key1")
	//assert.Contains(t, prettyOutput, "key2")
	//assert.Contains(t, prettyOutput, "key3")
}

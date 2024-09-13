package internal

import (
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlock(t *testing.T) {
	builder := NewBlockBuilder(4096)
	assert.True(t, builder.isEmpty())
	assert.True(t, builder.add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.True(t, builder.add([]byte("key2"), mo.Some([]byte("value2"))))
	assert.False(t, builder.isEmpty())

	block, err := builder.build()
	assert.Nil(t, err)

	encoded := block.encode()
	decoded := decodeBytesToBlock(encoded)
	assert.Equal(t, block.data, decoded.data)
	assert.Equal(t, block.offsets, decoded.offsets)
}

func TestBlockWithTombstone(t *testing.T) {
	builder := NewBlockBuilder(4096)
	assert.True(t, builder.add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.True(t, builder.add([]byte("key2"), mo.None[[]byte]()))
	assert.True(t, builder.add([]byte("key3"), mo.Some([]byte("value3"))))

	block, err := builder.build()
	assert.Nil(t, err)

	encoded := block.encode()
	decoded := decodeBytesToBlock(encoded)
	assert.Equal(t, block.data, decoded.data)
	assert.Equal(t, block.offsets, decoded.offsets)
}

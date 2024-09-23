package slatedb

import (
	"bytes"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlock(t *testing.T) {
	builder := newBlockBuilder(4096)
	assert.True(t, builder.isEmpty())
	assert.True(t, builder.add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.True(t, builder.add([]byte("key2"), mo.Some([]byte("value2"))))
	assert.False(t, builder.isEmpty())

	block, err := builder.build()
	assert.Nil(t, err)

	encoded := block.encodeToBytes()
	decoded := decodeBytesToBlock(encoded)
	assert.Equal(t, block.data, decoded.data)
	assert.Equal(t, block.offsets, decoded.offsets)
}

func TestBlockWithTombstone(t *testing.T) {
	builder := newBlockBuilder(4096)
	assert.True(t, builder.add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.True(t, builder.add([]byte("key2"), mo.None[[]byte]()))
	assert.True(t, builder.add([]byte("key3"), mo.Some([]byte("value3"))))

	block, err := builder.build()
	assert.Nil(t, err)

	encoded := block.encodeToBytes()
	decoded := decodeBytesToBlock(encoded)
	assert.Equal(t, block.data, decoded.data)
	assert.Equal(t, block.offsets, decoded.offsets)
}

func TestBlockIterator(t *testing.T) {
	kvPairs := []KeyValue{
		{[]byte("donkey"), []byte("kong")},
		{[]byte("kratos"), []byte("atreus")},
		{[]byte("super"), []byte("mario")},
	}

	builder := newBlockBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.add(kv.key, mo.Some(kv.value)))
	}

	block, err := builder.build()
	assert.Nil(t, err)

	iter := newBlockIteratorFromFirstKey(block)
	for i := 0; i < len(kvPairs); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		kv, ok := next.Get()
		assert.True(t, ok)
		assert.True(t, bytes.Equal(kv.key, kv.key))
		assert.True(t, bytes.Equal(kv.value, kv.value))
	}

	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, mo.None[KeyValue](), kv)
}

func TestIterFromExistingKey(t *testing.T) {
	kvPairs := []KeyValue{
		{[]byte("donkey"), []byte("kong")},
		{[]byte("kratos"), []byte("atreus")},
		{[]byte("super"), []byte("mario")},
	}

	builder := newBlockBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.add(kv.key, mo.Some(kv.value)))
	}

	block, err := builder.build()
	assert.Nil(t, err)

	iter := newBlockIteratorFromKey(block, []byte("kratos"))
	// Verify that iterator starts from index 1 which contains key "kratos"
	for i := 1; i < len(kvPairs); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		kv, ok := next.Get()
		assert.True(t, ok)
		assert.True(t, bytes.Equal(kv.key, kv.key))
		assert.True(t, bytes.Equal(kv.value, kv.value))
	}

	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, mo.None[KeyValue](), kv)
}

func TestIterFromNonExistingKey(t *testing.T) {
	kvPairs := []KeyValue{
		{[]byte("donkey"), []byte("kong")},
		{[]byte("kratos"), []byte("atreus")},
		{[]byte("super"), []byte("mario")},
	}

	builder := newBlockBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.add(kv.key, mo.Some(kv.value)))
	}

	block, err := builder.build()
	assert.Nil(t, err)

	iter := newBlockIteratorFromKey(block, []byte("ka"))
	// Verify that iterator starts from index 1 which contains key "kratos"
	for i := 1; i < len(kvPairs); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		kv, ok := next.Get()
		assert.True(t, ok)
		assert.True(t, bytes.Equal(kv.key, kv.key))
		assert.True(t, bytes.Equal(kv.value, kv.value))
	}

	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, mo.None[KeyValue](), kv)
}

func TestIterFromEnd(t *testing.T) {
	kvPairs := []KeyValue{
		{[]byte("donkey"), []byte("kong")},
		{[]byte("kratos"), []byte("atreus")},
		{[]byte("super"), []byte("mario")},
	}

	builder := newBlockBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.add(kv.key, mo.Some(kv.value)))
	}

	block, err := builder.build()
	assert.Nil(t, err)

	iter := newBlockIteratorFromKey(block, []byte("zzz"))
	// Verify that iterator starts from index 1 which contains key "kratos"
	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, mo.None[KeyValue](), kv)
}

package slatedb

import (
	"bytes"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
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
	kvPairs := []common.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	builder := newBlockBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.add(kv.Key, mo.Some(kv.Value)))
	}

	block, err := builder.build()
	assert.Nil(t, err)

	iter := newBlockIteratorFromFirstKey(block)
	for i := 0; i < len(kvPairs); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		kv, ok := next.Get()
		assert.True(t, ok)
		assert.True(t, bytes.Equal(kv.Key, kv.Key))
		assert.True(t, bytes.Equal(kv.Value, kv.Value))
	}

	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, mo.None[common.KV](), kv)
}

func TestIterFromExistingKey(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	builder := newBlockBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.add(kv.Key, mo.Some(kv.Value)))
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
		assert.True(t, bytes.Equal(kv.Key, kv.Key))
		assert.True(t, bytes.Equal(kv.Value, kv.Value))
	}

	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, mo.None[common.KV](), kv)
}

func TestIterFromNonExistingKey(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	builder := newBlockBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.add(kv.Key, mo.Some(kv.Value)))
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
		assert.True(t, bytes.Equal(kv.Key, kv.Key))
		assert.True(t, bytes.Equal(kv.Value, kv.Value))
	}

	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, mo.None[common.KV](), kv)
}

func TestIterFromEnd(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	builder := newBlockBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.add(kv.Key, mo.Some(kv.Value)))
	}

	block, err := builder.build()
	assert.Nil(t, err)

	iter := newBlockIteratorFromKey(block, []byte("zzz"))
	// Verify that iterator starts from index 1 which contains key "kratos"
	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, mo.None[common.KV](), kv)
}

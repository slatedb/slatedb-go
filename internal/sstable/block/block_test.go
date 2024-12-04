package block_test

import (
	"bytes"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewBuilder(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.IsEmpty())
	assert.True(t, bb.Add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.True(t, bb.Add([]byte("key2"), mo.Some([]byte("value2"))))
	assert.False(t, bb.IsEmpty())

	b, err := bb.Build()
	assert.NoError(t, err)

	encoded := block.Encode(b)
	assert.NoError(t, err)
	var decoded block.Block
	assert.NoError(t, block.Decode(&decoded, encoded))
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)
}

func TestBlock(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.IsEmpty())
	assert.True(t, bb.Add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.True(t, bb.Add([]byte("key2"), mo.Some([]byte("value2"))))
	assert.False(t, bb.IsEmpty())

	b, err := bb.Build()
	assert.Nil(t, err)

	encoded := block.Encode(b)
	var decoded block.Block
	require.NoError(t, block.Decode(&decoded, encoded))
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)
}

func TestBlockWithTombstone(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.Add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.True(t, bb.Add([]byte("key2"), mo.None[[]byte]()))
	assert.True(t, bb.Add([]byte("key3"), mo.Some([]byte("value3"))))

	b, err := bb.Build()
	assert.Nil(t, err)

	encoded := block.Encode(b)
	var decoded block.Block
	require.NoError(t, block.Decode(&decoded, encoded))
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)
}

func TestBlockIterator(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	bb := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, bb.Add(kv.Key, mo.Some(kv.Value)))
	}

	b, err := bb.Build()
	assert.Nil(t, err)

	iter := block.NewIterator(b)
	for i := 0; i < len(kvPairs); i++ {
		kvDel, shouldContinue := iter.NextEntry()
		assert.True(t, shouldContinue)
		assert.True(t, bytes.Equal(kvDel.Key, kvPairs[i].Key))
		assert.True(t, bytes.Equal(kvDel.ValueDel.Value, kvPairs[i].Value))
		assert.False(t, kvDel.ValueDel.IsTombstone)
	}

	kvDel, shouldContinue := iter.NextEntry()
	assert.False(t, shouldContinue)
	assert.Equal(t, common.KVDeletable{}, kvDel)
}

func TestNewIteratorAtKey(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	bb := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, bb.Add(kv.Key, mo.Some(kv.Value)))
	}

	b, err := bb.Build()
	assert.Nil(t, err)

	iter := block.NewIteratorAtKey(b, []byte("kratos"))
	// Verify that iterator starts from index 1 which contains key "kratos"
	for i := 1; i < len(kvPairs); i++ {
		kv, shouldContinue := iter.Next()
		assert.True(t, shouldContinue)
		assert.True(t, bytes.Equal(kv.Key, kvPairs[i].Key))
		assert.True(t, bytes.Equal(kv.Value, kvPairs[i].Value))
	}

	kv, shouldContinue := iter.Next()
	assert.False(t, shouldContinue)
	assert.Equal(t, common.KV{}, kv)
}

func TestNewIteratorAtKeyNonExistingKey(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	bb := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, bb.Add(kv.Key, mo.Some(kv.Value)))
	}

	b, err := bb.Build()
	assert.Nil(t, err)

	iter := block.NewIteratorAtKey(b, []byte("ka"))
	// Verify that iterator starts from index 1 which contains key "kratos"
	for i := 1; i < len(kvPairs); i++ {
		kvDel, shouldContinue := iter.NextEntry()
		assert.True(t, shouldContinue)
		assert.True(t, bytes.Equal(kvDel.Key, kvPairs[i].Key))
		assert.True(t, bytes.Equal(kvDel.ValueDel.Value, kvPairs[i].Value))
		assert.False(t, kvDel.ValueDel.IsTombstone)
	}

	kvDel, shouldContinue := iter.NextEntry()
	assert.False(t, shouldContinue)
	assert.Equal(t, common.KVDeletable{}, kvDel)
}

func TestIterFromEnd(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	bb := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, bb.Add(kv.Key, mo.Some(kv.Value)))
	}

	b, err := bb.Build()
	assert.Nil(t, err)

	iter := block.NewIteratorAtKey(b, []byte("zzz"))
	// Verify that iterator starts from index 1 which contains key "kratos"
	kv, ok := iter.Next()
	assert.False(t, ok)
	assert.Equal(t, common.KV{}, kv)
}

// TODO(thrawn01): Add additional tests <-- do this next

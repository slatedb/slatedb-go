package block_test

import (
	"bytes"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/compress"
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

	encoded, err := block.Encode(b, compress.CodecNone)
	assert.NoError(t, err)

	var decoded block.Block
	assert.NoError(t, block.Decode(&decoded, encoded, compress.CodecNone))
	assert.Equal(t, b.FirstKey, []byte("key1"))
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)
}

func TestBlockCompression(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.IsEmpty())
	assert.True(t, bb.Add([]byte("key1"), mo.Some([]byte("value1"))))
	assert.True(t, bb.Add([]byte("key2"), mo.Some([]byte("value2"))))
	assert.False(t, bb.IsEmpty())

	b, err := bb.Build()
	assert.Nil(t, err)

	encoded, err := block.Encode(b, compress.CodecLz4)
	assert.NoError(t, err)

	var decoded block.Block
	require.NoError(t, block.Decode(&decoded, encoded, compress.CodecLz4))
	assert.Equal(t, b.FirstKey, []byte("key1"))
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

	encoded, err := block.Encode(b, compress.CodecNone)
	assert.NoError(t, err)

	var decoded block.Block
	require.NoError(t, block.Decode(&decoded, encoded, compress.CodecNone))
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

func TestNewBuilderWithOffsets(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.IsEmpty())

	kvPairs := []common.KV{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("longerkey3"), Value: []byte("longervalue3")},
		{Key: []byte("k4"), Value: []byte("v4")},
	}

	for _, kv := range kvPairs {
		assert.True(t, bb.Add(kv.Key, mo.Some(kv.Value)))
	}

	b, err := bb.Build()
	assert.NoError(t, err)

	//t.Log("Block Offsets:")
	//for i, offset := range b.Offsets {
	//	t.Logf("Entry %d: Offset %d", i, offset)
	//}

	// Verify the number of entries
	assert.Equal(t, len(kvPairs), len(b.Offsets))

	// Verify that offsets are in ascending order
	for i := 1; i < len(b.Offsets); i++ {
		assert.True(t, b.Offsets[i] > b.Offsets[i-1], "Offsets should be in ascending order")
	}
}

func TestTruncate(t *testing.T) {
	testCases := []struct {
		input    []byte
		maxLen   int
		expected string
	}{
		{[]byte("short"), 10, "short"},
		{[]byte("longer string"), 10, "longer ..."},
		{[]byte("exactly10"), 10, "exactly10"},
		{[]byte("11characters"), 10, "11chara..."},
	}

	for _, tc := range testCases {
		result := block.Truncate(tc.input, tc.maxLen)
		assert.Equal(t, tc.expected, result)
	}
}

func TestPrettyPrint(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.Add([]byte("database"), mo.Some([]byte("internals"))))
	assert.True(t, bb.Add([]byte("data-intensive"), mo.Some([]byte("applications"))))
	assert.True(t, bb.Add([]byte("deleted"), mo.Some([]byte(""))))

	b, err := bb.Build()
	assert.NoError(t, err)
	out := block.PrettyPrint(b)

	//t.Log(out)
	assert.Contains(t, out, "database")
	assert.Contains(t, out, "internals")
	assert.Contains(t, out, "data-intensive")
	assert.Contains(t, out, "applications")
	assert.Contains(t, out, "deleted")
}

func TestBlockFirstKey(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.IsEmpty())

	kvPairs := []common.KV{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("longerkey3"), Value: []byte("longervalue3")},
		{Key: []byte("k4"), Value: []byte("v4")},
	}

	for _, kv := range kvPairs {
		assert.True(t, bb.Add(kv.Key, mo.Some(kv.Value)))
	}

	b, err := bb.Build()
	require.NoError(t, err)

	assert.Equal(t, []byte("key1"), b.FirstKey)
}

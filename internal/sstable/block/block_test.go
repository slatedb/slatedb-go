package block_test

import (
	"bytes"
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewBuilder(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.IsEmpty())
	assert.True(t, bb.AddValue([]byte("key1"), []byte("value1")))
	assert.True(t, bb.AddValue([]byte("key2"), []byte("value2")))
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
	assert.True(t, bb.AddValue([]byte("key1"), []byte("value1")))
	assert.True(t, bb.AddValue([]byte("key2"), []byte("value2")))
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

func TestSmallestCompressedBlock(t *testing.T) {
	testCases := []struct {
		codec compress.Codec
	}{
		{compress.CodecNone},
		{compress.CodecLz4},
		{compress.CodecSnappy},
		{compress.CodecZstd},
		{compress.CodecZlib},
	}

	for _, tc := range testCases {
		bb := block.NewBuilder(4096)
		assert.True(t, bb.IsEmpty())
		assert.True(t, bb.AddValue([]byte("k"), nil))
		assert.False(t, bb.IsEmpty())

		b, err := bb.Build()
		assert.Nil(t, err)

		encoded, err := block.Encode(b, tc.codec)
		assert.NoError(t, err)

		var decoded block.Block
		require.NoError(t, block.Decode(&decoded, encoded, tc.codec))
		assert.Equal(t, b.FirstKey, []byte("k"))
		assert.Equal(t, b.Data, decoded.Data)
		//t.Logf("Compression '%s' results in block size: %d", tc.codec.String(), len(decoded.Data))
		assert.True(t, len(b.Data) > 6)
		assert.Equal(t, b.Offsets, decoded.Offsets)
		it := block.NewIterator(&decoded)
		assert2.NextEntry(t, it, []byte("k"), nil)
	}

}

func TestBlockWithTombstone(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.AddValue([]byte("key1"), []byte("value1")))
	assert.True(t, bb.AddValue([]byte("key2"), []byte("")))
	assert.True(t, bb.AddValue([]byte("key3"), []byte("value3")))

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
	kvPairs := []types.KeyValue{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	bb := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, bb.AddValue(kv.Key, kv.Value))
	}

	b, err := bb.Build()
	assert.Nil(t, err)

	iter := block.NewIterator(b)
	for i := 0; i < len(kvPairs); i++ {
		entry, ok := iter.NextEntry()
		assert.True(t, ok)
		assert.Equal(t, kvPairs[i].Key, entry.Key)
		assert.Equal(t, kvPairs[i].Value, entry.Value.Value)
		assert.False(t, entry.Value.IsTombstone())
	}

	kvDel, ok := iter.NextEntry()
	assert.False(t, ok)
	assert.Equal(t, types.RowEntry{}, kvDel)
}

func TestNewIteratorAtKey(t *testing.T) {
	kvPairs := []types.KeyValue{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	bb := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, bb.AddValue(kv.Key, kv.Value))
	}

	b, err := bb.Build()
	require.NoError(t, err)

	t.Run("NotFirstKey", func(t *testing.T) {
		iter, err := block.NewIteratorAtKey(b, []byte("kratos"))
		require.NoError(t, err)

		// Verify that iterator starts from index 1 which contains key "kratos"
		for i := 1; i < len(kvPairs); i++ {
			kv, ok := iter.Next()
			assert.True(t, ok)
			assert.True(t, bytes.Equal(kv.Key, kvPairs[i].Key))
			assert.True(t, bytes.Equal(kv.Value, kvPairs[i].Value))
		}

		kv, ok := iter.Next()
		assert.False(t, ok)
		assert.Equal(t, types.KeyValue{}, kv)
	})

	t.Run("FirstKey", func(t *testing.T) {
		iter, err := block.NewIteratorAtKey(b, []byte("donkey"))
		require.NoError(t, err)

		// Verify that iterator starts from index 0 which contains key "donkey"
		for i := 0; i < len(kvPairs); i++ {
			kv, ok := iter.Next()
			assert.True(t, ok)
			assert.True(t, bytes.Equal(kv.Key, kvPairs[i].Key))
			assert.True(t, bytes.Equal(kv.Value, kvPairs[i].Value))
		}

		kv, ok := iter.Next()
		assert.False(t, ok)
		assert.Equal(t, types.KeyValue{}, kv)
	})
}

func TestNewIteratorAtKeyNonExistingKey(t *testing.T) {
	kvPairs := []types.KeyValue{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	bb := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, bb.AddValue(kv.Key, kv.Value))
	}

	b, err := bb.Build()
	assert.NoError(t, err)

	iter, err := block.NewIteratorAtKey(b, []byte("ka"))
	require.NoError(t, err)

	// Verify that iterator starts from index 1 which contains key "kratos"
	// which is near to `ka`
	for i := 1; i < len(kvPairs); i++ {
		kvDel, ok := iter.NextEntry()
		assert.True(t, ok)
		assert.True(t, bytes.Equal(kvDel.Key, kvPairs[i].Key))
		assert.True(t, bytes.Equal(kvDel.Value.Value, kvPairs[i].Value))
		assert.False(t, kvDel.Value.IsTombstone())
	}

	kvDel, ok := iter.NextEntry()
	assert.False(t, ok)
	assert.Equal(t, types.RowEntry{}, kvDel)
}

func TestIterFromEnd(t *testing.T) {
	kvPairs := []types.KeyValue{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	bb := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, bb.AddValue(kv.Key, kv.Value))
	}

	b, err := bb.Build()
	assert.NoError(t, err)

	iter, err := block.NewIteratorAtKey(b, []byte("zzz"))
	require.NoError(t, err)
	// Verify that iterator starts from index 1 which contains key "kratos"
	kv, ok := iter.Next()
	assert.False(t, ok)
	assert.Equal(t, types.KeyValue{}, kv)
}

func TestNewBuilderWithOffsets(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.IsEmpty())

	kvPairs := []types.KeyValue{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("longerkey3"), Value: []byte("longervalue3")},
		{Key: []byte("k4"), Value: []byte("v4")},
	}

	for _, kv := range kvPairs {
		assert.True(t, bb.AddValue(kv.Key, kv.Value))
	}

	b, err := bb.Build()
	assert.NoError(t, err)

	//t.Log("Block Offsets:")
	//for i, offset := range b.Offsets {
	//	t.Logf("RowEntry %d: Offset %d", i, offset)
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
	assert.True(t, bb.AddValue([]byte("database"), []byte("internals")))
	assert.True(t, bb.AddValue([]byte("data-intensive"), []byte("applications")))
	assert.True(t, bb.AddValue([]byte("deleted"), []byte("")))

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

	kvPairs := []types.KeyValue{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("longerkey3"), Value: []byte("longervalue3")},
		{Key: []byte("k4"), Value: []byte("v4")},
	}

	for _, kv := range kvPairs {
		assert.True(t, bb.AddValue(kv.Key, kv.Value))
	}

	b, err := bb.Build()
	require.NoError(t, err)

	assert.Equal(t, []byte("key1"), b.FirstKey)
}

func TestNewIteratorAtKeyWithCorruptedKeys(t *testing.T) {

	t.Run("AllKeysCorrupted", func(t *testing.T) {
		bb := block.NewBuilder(4096)
		kvPairs := []types.KeyValue{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		}
		for _, kv := range kvPairs {
			assert.True(t, bb.AddValue(kv.Key, kv.Value))
		}
		b, err := bb.Build()
		require.NoError(t, err)

		// Corrupt all keys
		for _, offset := range b.Offsets {
			b.Data[offset] = 0xFF
		}

		_, err = block.NewIteratorAtKey(b, []byte("key1"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to locate uncorrupted first key in block; block is corrupt")
	})

	t.Run("AllKeysCorruptedFirstKeyCorrupt", func(t *testing.T) {
		bb := block.NewBuilder(4096)
		kvPairs := []types.KeyValue{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
			{Key: []byte("key3"), Value: []byte("value3")},
			{Key: []byte("key4"), Value: []byte("value4")},
			{Key: []byte("key5"), Value: []byte("value5")},
		}
		for _, kv := range kvPairs {
			assert.True(t, bb.AddValue(kv.Key, kv.Value))
		}
		b, err := bb.Build()
		require.NoError(t, err)

		// Corrupt the first key
		b.Data[0] = 0xFF

		// Because all the keys in the block share the same prefix
		// the subsequent keys cannot be reconstructed. As a result we
		// are unable to find "key4".
		_, err = block.NewIteratorAtKey(b, []byte("key4"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to locate uncorrupted first key in block; block is corrupt")
	})

	t.Run("CorruptedFirstKey", func(t *testing.T) {
		bb := block.NewBuilder(4096)
		kvPairs := []types.KeyValue{
			{Key: []byte("hello"), Value: []byte("world")},
			{Key: []byte("rainbow"), Value: []byte("dash")},
			{Key: []byte("wonderful"), Value: []byte("day")},
		}
		for _, kv := range kvPairs {
			assert.True(t, bb.AddValue(kv.Key, kv.Value))
		}
		b, err := bb.Build()
		require.NoError(t, err)

		// Corrupt the first key
		b.Data[b.Offsets[0]] = 0xFF // This will make the first key invalid

		iter, err := block.NewIteratorAtKey(b, []byte("key1"))
		require.NoError(t, err)

		// The iterator should start from the second key as the second
		// key is a full key -- because it doesn't share a prefix with
		// the previous key.
		assert2.Next(t, iter, []byte("rainbow"), []byte("dash"))
		// Should iterate to the next key
		assert2.Next(t, iter, []byte("wonderful"), []byte("day"))

		// Assert there were warnings
		assert.False(t, iter.Warnings().Empty())
		//t.Logf("Warnings: %s", iter.Warnings().String())
	})

	t.Run("SomeKeysCorrupted", func(t *testing.T) {
		bb := block.NewBuilder(4096)
		kvPairs := []types.KeyValue{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
			{Key: []byte("key3"), Value: []byte("value3")},
			{Key: []byte("key4"), Value: []byte("value4")},
			{Key: []byte("key5"), Value: []byte("value5")},
		}
		for _, kv := range kvPairs {
			assert.True(t, bb.AddValue(kv.Key, kv.Value))
		}
		b, err := bb.Build()
		require.NoError(t, err)

		// Corrupt key2 and key3
		b.Data[b.Offsets[1]] = 0xFF
		b.Data[b.Offsets[2]] = 0xFF

		iter, err := block.NewIteratorAtKey(b, []byte("key4"))
		require.NoError(t, err)

		// The iterator should start from the fourth key
		assert2.Next(t, iter, []byte("key4"), []byte("value4"))
		assert2.Next(t, iter, []byte("key5"), []byte("value5"))
		_, ok := iter.Next()
		assert.False(t, ok)
		assert.False(t, iter.Warnings().Empty())
		//t.Logf("Warnings: %s", iter.Warnings())
	})
}

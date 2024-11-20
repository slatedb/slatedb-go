package table

import (
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWALOps(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("abc111"), Value: []byte("value1")},
		{Key: []byte("abc222"), Value: []byte("value2")},
		{Key: []byte("abc333"), Value: []byte("value3")},
	}

	wal := NewWAL()
	assert.Equal(t, int64(0), wal.Size())

	var size int64
	// Put KV pairs and verify Get
	for _, kvPair := range kvPairs {
		size += wal.Put(kvPair.Key, kvPair.Value)
	}
	for _, kvPair := range kvPairs {
		assert.Equal(t, kvPair.Value, wal.Get(kvPair.Key).MustGet().Value)
	}
	assert.Equal(t, size, wal.Size())

	// Delete KV and verify that it is tombstoned
	wal.Delete(kvPairs[1].Key)
	assert.True(t, wal.Get(kvPairs[1].Key).MustGet().IsTombstone)
}

func TestWALIter(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("abc111"), Value: []byte("value1")},
		{Key: []byte("abc222"), Value: []byte("value2")},
		{Key: []byte("abc333"), Value: []byte("value3")},
		{Key: []byte("abc444"), Value: []byte("value4")},
		{Key: []byte("abc555"), Value: []byte("value5")},
	}

	wal := NewWAL()

	// Put keys in random order
	indexes := []int{2, 0, 4, 3, 1}
	for i := range indexes {
		wal.Put(kvPairs[i].Key, kvPairs[i].Value)
	}

	iter := wal.Iter()

	// Verify that iterator returns keys in sorted order
	for i := 0; i < len(kvPairs); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		kv, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, kvPairs[i].Key, kv.Key)
		assert.Equal(t, kvPairs[i].Value, kv.Value)
	}
}

func TestWALIterDelete(t *testing.T) {
	wal := NewWAL()

	wal.Put([]byte("abc333"), []byte("value3"))
	next, err := wal.Iter().Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())

	wal.Delete([]byte("abc333"))
	next, err = wal.Iter().Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestImmWALOps(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("abc111"), Value: []byte("value1")},
		{Key: []byte("abc222"), Value: []byte("value2")},
		{Key: []byte("abc333"), Value: []byte("value3")},
	}

	wal := NewWAL()
	// Put KV pairs to wal
	for _, kvPair := range kvPairs {
		wal.Put(kvPair.Key, kvPair.Value)
	}

	// create ImmutableWal from wal and verify Get
	immWAL := NewImmutableWal(wal, 1)
	for _, kvPair := range kvPairs {
		assert.Equal(t, kvPair.Value, immWAL.Get(kvPair.Key).MustGet().Value)
	}
	assert.Equal(t, uint64(1), immWAL.ID())

	iter := immWAL.Iter()

	// Verify that iterator returns keys in sorted order
	for i := 0; i < len(kvPairs); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		kv, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, kvPairs[i].Key, kv.Key)
		assert.Equal(t, kvPairs[i].Value, kv.Value)
	}
}

package table

import (
	"bytes"
	"testing"

	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestWALOps(t *testing.T) {
	kvPairs := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
	}

	wal := NewWAL()
	assert.Equal(t, int64(0), wal.Size())

	var size int64
	// Put RowEntry pairs
	for _, entry := range kvPairs {
		size += wal.Put(entry)
	}
	// verify Get values for all RowEntry pairs
	for _, entry := range kvPairs {
		assert.Equal(t, entry.Value, wal.Get(entry.Key).MustGet())
	}
	assert.Equal(t, size, wal.Size())

	// Put a tombstone and verify that it is tombstoned
	wal.Put(types.RowEntry{Key: kvPairs[1].Key, Value: types.Value{Kind: types.KindTombStone}})
	assert.True(t, wal.Get(kvPairs[1].Key).MustGet().IsTombstone())
}

func TestWALIter(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
		{Key: []byte("abc444"), Value: types.Value{Value: []byte("value4"), Kind: types.KindKeyValue}},
		{Key: []byte("abc555"), Value: types.Value{Value: []byte("value5"), Kind: types.KindKeyValue}},
	}

	wal := NewWAL()

	// Put entries in random order
	indexes := []int{2, 0, 4, 3, 1}
	for _, i := range indexes {
		wal.Put(entries[i])
	}

	iter := wal.Iter()

	// Verify that iterator returns entries in sorted order
	for i := 0; i < len(entries); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		entry, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, entries[i].Key, entry.Key)
		assert.Equal(t, entries[i].Value.Value, entry.Value.Value)
		assert.Equal(t, entries[i].Value.Kind, entry.Value.Kind)
	}
}

func TestWALIterTombstone(t *testing.T) {
	wal := NewWAL()

	// verify that iter.Next() is present after adding a key
	wal.Put(types.RowEntry{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}})
	next, err := wal.Iter().Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())

	// verify that iter.Next() returns a tombstone after putting a tombstone
	wal.Put(types.RowEntry{Key: []byte("abc333"), Value: types.Value{Kind: types.KindTombStone}})
	next, err = wal.Iter().Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())
	entry, ok := next.Get()
	assert.True(t, ok)
	assert.True(t, entry.Value.IsTombstone())
}

func TestImmWALOps(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
	}

	wal := NewWAL()
	// Put RowEntry pairs to wal
	for _, entry := range entries {
		wal.Put(entry)
	}

	// create ImmutableWAL from WAL and verify Get values for all RowEntry pairs
	immWAL := NewImmutableWAL(wal, 1)
	for _, entry := range entries {
		assert.Equal(t, entry.Value, immWAL.Get(entry.Key).MustGet())
	}
	assert.Equal(t, uint64(1), immWAL.ID())

	iter := immWAL.Iter()

	// Verify that iterator returns entries in sorted order
	for i := 0; i < len(entries); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		entry, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, entries[i].Key, entry.Key)
		assert.Equal(t, entries[i].Value.Value, entry.Value.Value)
		assert.Equal(t, entries[i].Value.Kind, entry.Value.Kind)
	}
}

func TestWALClone(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
	}

	wal := NewWAL()
	// Put RowEntry pairs to wal
	for _, entry := range entries {
		wal.Put(entry)
	}

	clonedWAL := wal.Clone()
	// verify that the contents are equal
	assert.True(t, bytes.Equal(wal.table.toBytes(), clonedWAL.table.toBytes()))

	// verify that the clone does not point to same data in memory
	key := []byte("abc333")
	newValue := types.Value{Value: []byte("newValue"), Kind: types.KindKeyValue}
	wal.Put(types.RowEntry{Key: key, Value: newValue})
	assert.Equal(t, newValue, wal.Get(key).MustGet())
	assert.NotEqual(t, newValue, clonedWAL.Get(key).MustGet())

	immWAL := NewImmutableWAL(wal, 1)
	clonedImmWAL := immWAL.Clone()
	// verify that the contents are equal
	assert.Equal(t, immWAL.ID(), clonedImmWAL.ID())
	assert.True(t, bytes.Equal(immWAL.table.toBytes(), clonedImmWAL.table.toBytes()))
}

package table

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/slatedb/slatedb-go/internal/types"
)

func TestMemtableOps(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
	}

	memtable := NewMemtable()

	var size int64
	// Put entries
	for _, e := range entries {
		size += memtable.Put(e)
	}
	// verify Get for all the entries
	for _, e := range entries {
		assert.Equal(t, e.Value.Value, memtable.Get(e.Key).MustGet().Value)
	}
	assert.Equal(t, size, memtable.Size())

	// Delete and verify that it is tombstoned
	memtable.Put(types.RowEntry{
		Key: entries[1].Key,
		Value: types.Value{
			Kind: types.KindTombStone,
		},
	})
	assert.True(t, memtable.Get(entries[1].Key).MustGet().IsTombstone())

	memtable.SetLastWalID(1)
	assert.Equal(t, uint64(1), memtable.LastWalID().MustGet())
}

func TestMemtableIter(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
		{Key: []byte("abc444"), Value: types.Value{Value: []byte("value4"), Kind: types.KindKeyValue}},
		{Key: []byte("abc555"), Value: types.Value{Value: []byte("value5"), Kind: types.KindKeyValue}},
	}

	memtable := NewMemtable()

	// Put keys in random order
	indexes := []int{2, 0, 4, 3, 1}
	for i := range indexes {
		memtable.Put(entries[i])
	}

	iter := memtable.Iter()

	// Verify that iterator returns keys in sorted order
	for i := 0; i < len(entries); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		e, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, entries[i].Key, e.Key)
		assert.Equal(t, entries[i].Value.Value, e.Value.Value)
		assert.Equal(t, entries[i].Value.Kind, e.Value.Kind)
	}
}

func TestMemtableIterTombstone(t *testing.T) {
	memtable := NewMemtable()

	// verify that iter.Next() is present after adding a key
	memtable.Put(types.RowEntry{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}})
	next, err := memtable.Iter().Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())

	// verify that iter.Next() returns tombstone after marking a key as deleted
	memtable.Put(types.RowEntry{Key: []byte("abc333"), Value: types.Value{Kind: types.KindTombStone}})
	next, err = memtable.Iter().Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())
	kv, ok := next.Get()
	assert.True(t, ok)
	assert.True(t, kv.Value.IsTombstone())
}

func TestMemtableRangeFromExistingKey(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
		{Key: []byte("abc444"), Value: types.Value{Value: []byte("value4"), Kind: types.KindKeyValue}},
		{Key: []byte("abc555"), Value: types.Value{Value: []byte("value5"), Kind: types.KindKeyValue}},
	}

	memtable := NewMemtable()

	// Put keys in random order
	indexes := []int{2, 0, 4, 3, 1}
	for i := range indexes {
		memtable.Put(entries[indexes[i]])
	}

	iter := memtable.RangeFrom([]byte("abc333"))

	// Verify that iterator starts from index 2 which contains key abc333
	for i := 2; i < len(entries); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		e, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, entries[i].Key, e.Key)
		assert.Equal(t, entries[i].Value.Value, e.Value.Value)
		assert.Equal(t, entries[i].Value.Kind, e.Value.Kind)
	}
}

func TestMemtableRangeFromNonExistingKey(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
		{Key: []byte("abc444"), Value: types.Value{Value: []byte("value4"), Kind: types.KindKeyValue}},
		{Key: []byte("abc555"), Value: types.Value{Value: []byte("value5"), Kind: types.KindKeyValue}},
	}

	memtable := NewMemtable()

	// Put keys in random order
	indexes := []int{2, 0, 4, 3, 1}
	for i := range indexes {
		memtable.Put(entries[indexes[i]])
	}

	iter := memtable.RangeFrom([]byte("abc345"))

	// Verify that iterator starts from index 3 which contains key abc444
	for i := 3; i < len(entries); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		e, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, entries[i].Key, e.Key)
		assert.Equal(t, entries[i].Value.Value, e.Value.Value)
		assert.Equal(t, entries[i].Value.Kind, e.Value.Kind)
	}
}

func TestImmMemtableOps(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
	}

	memtable := NewMemtable()
	// Put keys in random order
	indexes := []int{1, 2, 0}
	for i := range indexes {
		memtable.Put(entries[indexes[i]])
	}

	// create ImmutableMemtable from memtable and verify Get values for all entries
	immMemtable := NewImmutableMemtable(memtable, 1)
	for _, entry := range entries {
		assert.Equal(t, entry.Value.Value, immMemtable.Get(entry.Key).MustGet().Value)
	}
	assert.Equal(t, uint64(1), immMemtable.LastWalID())

	iter := immMemtable.Iter()

	// Verify that iterator returns keys in sorted order
	for i := 0; i < len(entries); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		e, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, entries[i].Key, e.Key)
		assert.Equal(t, entries[i].Value.Value, e.Value.Value)
		assert.Equal(t, entries[i].Value.Kind, e.Value.Kind)
	}
}

func TestMemtableClone(t *testing.T) {
	entries := []types.RowEntry{
		{Key: []byte("abc111"), Value: types.Value{Value: []byte("value1"), Kind: types.KindKeyValue}},
		{Key: []byte("abc222"), Value: types.Value{Value: []byte("value2"), Kind: types.KindKeyValue}},
		{Key: []byte("abc333"), Value: types.Value{Value: []byte("value3"), Kind: types.KindKeyValue}},
	}

	memtable := NewMemtable()
	// Put RowEntry pairs to memtable
	for _, entry := range entries {
		memtable.Put(entry)
	}
	memtable.SetLastWalID(1)

	clonedMemtable := memtable.Clone()
	// verify that the contents are equal
	assert.Equal(t, memtable.LastWalID(), clonedMemtable.LastWalID())
	assert.True(t, bytes.Equal(memtable.table.toBytes(), clonedMemtable.table.toBytes()))

	// verify that the clone does not point to same data in memory
	key := []byte("abc333")
	newValue := types.Value{Value: []byte("newValue"), Kind: types.KindKeyValue}
	memtable.Put(types.RowEntry{Key: key, Value: newValue})
	assert.Equal(t, newValue.Value, memtable.Get(key).MustGet().Value)
	assert.NotEqual(t, newValue.Value, clonedMemtable.Get(key).MustGet().Value)

	immMemtable := NewImmutableMemtable(memtable, 1)
	clonedImmMemtable := immMemtable.Clone()
	// verify that the contents are equal
	assert.Equal(t, immMemtable.LastWalID(), clonedImmMemtable.LastWalID())
	assert.True(t, bytes.Equal(immMemtable.table.toBytes(), clonedImmMemtable.table.toBytes()))
}

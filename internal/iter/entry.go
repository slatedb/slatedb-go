package iter

import (
	"github.com/slatedb/slatedb-go/internal/types"
)

type KVIterator interface {
	// Next Returns the next non-deleted key-value pair in the iterator.
	Next() (types.KeyValue, bool)

	// NextEntry Returns the next entry in the iterator, which may be a key-value pair or
	// a tombstone of a deleted key-value pair.
	NextEntry() (types.RowEntry, bool)
}

type EntryIterator struct {
	entries []types.RowEntry
	index   int
}

// NewEntryIterator is an iterator made up of RowEntry items. Users can add RowEntry items
// via the NewEntryIterator() constructor or by calling Add() on the iterator.
// TODO(thrawn01): Rename this when we rename RowEntry
func NewEntryIterator(entries ...types.RowEntry) *EntryIterator {
	return &EntryIterator{
		entries: entries,
		index:   0,
	}
}

func (k *EntryIterator) Next() (types.KeyValue, bool) {
	for k.index < len(k.entries) {
		entry := k.entries[k.index]
		k.index++
		if !entry.Value.IsTombstone() {
			return types.KeyValue{Key: entry.Key, Value: entry.Value.Value}, true
		}
	}
	return types.KeyValue{}, false
}

func (k *EntryIterator) NextEntry() (types.RowEntry, bool) {
	if k.index < len(k.entries) {
		entry := k.entries[k.index]
		k.index++
		return entry, true
	}
	return types.RowEntry{}, false
}

func (k *EntryIterator) Add(key []byte, value []byte) *EntryIterator {
	k.entries = append(k.entries, types.RowEntry{
		Key: key,
		Value: types.Value{
			Value: value,
		},
	})
	return k
}

func (k *EntryIterator) Len() int {
	return len(k.entries)
}

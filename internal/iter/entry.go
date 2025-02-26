package iter

import (
	"context"

	"github.com/slatedb/slatedb-go/internal/types"
)

type KVIterator interface {
	// NextEntry Returns the next entry in the iterator, which may be a key-value pair or
	// a tombstone of a deleted key-value pair.
	NextEntry(context.Context) (types.RowEntry, bool)

	// Warnings returns any warnings issued during iteration which should be logged by the caller
	Warnings() *types.ErrWarn
}

type EntryIterator struct {
	entries []types.RowEntry
	index   int
}

// NewEntryIterator is an iterator made up of RowEntry items. Users can add RowEntry items
// via the NewEntryIterator() constructor or by calling Add() on the iterator.
func NewEntryIterator(entries ...types.RowEntry) *EntryIterator {
	return &EntryIterator{
		entries: entries,
		index:   0,
	}
}

func (k *EntryIterator) NextEntry(ctx context.Context) (types.RowEntry, bool) {
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

// Warnings returns types.ErrWarn if there was a warning during iteration.
func (k *EntryIterator) Warnings() *types.ErrWarn {
	return nil
}

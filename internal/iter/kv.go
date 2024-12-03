package iter

import (
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

type KVIterator interface {
	// Next Returns the next non-deleted key-value pair in the iterator.
	Next() (common.KV, bool)

	// NextEntry Returns the next entry in the iterator, which may be a key-value pair or
	// a tombstone of a deleted key-value pair.
	// TODO: Rename this when we rename KVDeletable
	NextEntry() (common.KVDeletable, bool)
}

type KV struct {
	entries []common.KVDeletable
	index   int
}

// NewKV is an iterator made up of KVDeletable items. Users can add KVDeletable items
// via the NewKV() constructor or by calling Add() on the iterator.
// TODO(thrawn01): Rename this when we rename KVDeletable
func NewKV(kv ...common.KVDeletable) *KV {
	return &KV{
		entries: kv,
		index:   0,
	}
}

func (k *KV) Next() (common.KV, bool) {
	for k.index < len(k.entries) {
		entry := k.entries[k.index]
		k.index++
		if !entry.ValueDel.IsTombstone {
			return common.KV{Key: entry.Key, Value: entry.ValueDel.Value}, true
		}
	}
	return common.KV{}, false
}

func (k *KV) NextEntry() (common.KVDeletable, bool) {
	if k.index < len(k.entries) {
		entry := k.entries[k.index]
		k.index++
		return entry, true
	}
	return common.KVDeletable{}, false
}

func (k *KV) Add(key []byte, value []byte) *KV {
	k.entries = append(k.entries, common.KVDeletable{
		Key: key,
		ValueDel: common.ValueDeletable{
			Value:       value,
			IsTombstone: false,
		},
	})
	return k
}

func (k *KV) Len() int {
	return len(k.entries)
}

// AssertNextEntry is a test helper to assert the next call to NextEntry() returns the required value
func AssertNextEntry(t *testing.T, iter KVIterator, key []byte, value []byte) {
	t.Helper()
	entry, ok := iter.NextEntry()
	assert.True(t, ok)
	assert.Equal(t, key, entry.Key)
	if value == nil {
		assert.True(t, entry.ValueDel.IsTombstone)
		assert.Equal(t, []byte(nil), entry.ValueDel.Value)
	} else {
		assert.False(t, entry.ValueDel.IsTombstone)
		assert.Equal(t, value, entry.ValueDel.Value)
	}
}

// AssertNext is a test helper to assert the next call to Next() returns the required value
func AssertNext(t *testing.T, iter KVIterator, key []byte, value []byte) bool {
	t.Helper()
	kv, _ := iter.Next()
	//assert.True(t, ok)
	if !assert.Equal(t, key, kv.Key) {
		return false
	}
	if !assert.Equal(t, value, kv.Value) {
		return false
	}
	return true
}

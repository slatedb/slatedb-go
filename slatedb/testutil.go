package slatedb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type kv struct {
	key   []byte
	value []byte
}

func assertIterNextEntry(t *testing.T, iter KeyValueIterator, key []byte, value []byte) {
	nextEntry, err := iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsPresent())

	entry, _ := nextEntry.Get()
	assert.Equal(t, key, entry.key)
	if value == nil {
		assert.True(t, entry.valueDel.isTombstone)
		assert.Equal(t, []byte(nil), entry.valueDel.value)
	} else {
		assert.False(t, entry.valueDel.isTombstone)
		assert.Equal(t, value, entry.valueDel.value)
	}
}

func assertIterNext(t *testing.T, iter KeyValueIterator, key []byte, value []byte) {
	next, err := iter.Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())

	kv, _ := next.Get()
	assert.Equal(t, key, kv.key)
	assert.Equal(t, value, kv.value)
}

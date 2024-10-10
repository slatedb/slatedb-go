package iter

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func AssertIterNextEntry(t *testing.T, iter KVIterator, key []byte, value []byte) {
	nextEntry, err := iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsPresent())

	entry, _ := nextEntry.Get()
	assert.Equal(t, key, entry.Key)
	if value == nil {
		assert.True(t, entry.ValueDel.IsTombstone)
		assert.Equal(t, []byte(nil), entry.ValueDel.Value)
	} else {
		assert.False(t, entry.ValueDel.IsTombstone)
		assert.Equal(t, value, entry.ValueDel.Value)
	}
}

func AssertIterNext(t *testing.T, iter KVIterator, key []byte, value []byte) {
	next, err := iter.Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())

	kv, _ := next.Get()
	assert.Equal(t, key, kv.Key)
	assert.Equal(t, value, kv.Value)
}

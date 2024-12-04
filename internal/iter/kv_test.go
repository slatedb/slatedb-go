package iter_test

import (
	"github.com/slatedb/slatedb-go/internal/iter"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewKV(t *testing.T) {
	// Test creating a new empty iterator
	it := iter.NewKV()
	assert.NotNil(t, it)
	assert.Equal(t, 0, it.Len())

	// Test adding entries
	it.Add([]byte("key1"), []byte("value1"))
	it.Add([]byte("key2"), []byte("value2"))

	assert.Equal(t, 2, it.Len())

	// Verify the first entry
	entry1, ok := it.NextEntry()
	assert.True(t, ok)
	assert.Equal(t, []byte("key1"), entry1.Key)
	assert.Equal(t, []byte("value1"), entry1.ValueDel.Value)
	assert.False(t, entry1.ValueDel.IsTombstone)

	// Verify the second entry
	entry2, ok := it.NextEntry()
	assert.True(t, ok)
	assert.Equal(t, []byte("key2"), entry2.Key)
	assert.Equal(t, []byte("value2"), entry2.ValueDel.Value)
	assert.False(t, entry2.ValueDel.IsTombstone)

	// Verify that the iterator is now empty
	_, ok = it.NextEntry()
	assert.False(t, ok)
}

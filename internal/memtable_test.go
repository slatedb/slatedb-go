package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type kv struct {
	key   []byte
	value []byte
}

func TestMemtableIter(t *testing.T) {
	kvPairs := []kv{
		{[]byte("abc111"), []byte("value1")},
		{[]byte("abc222"), []byte("value2")},
		{[]byte("abc333"), []byte("value3")},
		{[]byte("abc444"), []byte("value4")},
		{[]byte("abc555"), []byte("value5")},
	}

	table := newWritableKVTable()

	// Put keys in random order
	table.put(kvPairs[2].key, kvPairs[2].value)
	table.put(kvPairs[0].key, kvPairs[0].value)
	table.put(kvPairs[4].key, kvPairs[4].value)
	table.put(kvPairs[3].key, kvPairs[3].value)
	table.put(kvPairs[1].key, kvPairs[1].value)

	iter := table.table.iter()

	// Verify that iterator returns keys in sorted order
	for i := 0; i < len(kvPairs); i++ {
		kv, ok := iter.Next().Get()
		assert.True(t, ok)
		assert.Equal(t, kvPairs[i].key, kv.key)
		assert.Equal(t, kvPairs[i].value, kv.value)
	}
}

func TestMemtableRangeFromExistingKey(t *testing.T) {
	kvPairs := []kv{
		{[]byte("abc111"), []byte("value1")},
		{[]byte("abc222"), []byte("value2")},
		{[]byte("abc333"), []byte("value3")},
		{[]byte("abc444"), []byte("value4")},
		{[]byte("abc555"), []byte("value5")},
	}

	table := newWritableKVTable()

	// Put keys in random order
	table.put(kvPairs[2].key, kvPairs[2].value)
	table.put(kvPairs[0].key, kvPairs[0].value)
	table.put(kvPairs[4].key, kvPairs[4].value)
	table.put(kvPairs[3].key, kvPairs[3].value)
	table.put(kvPairs[1].key, kvPairs[1].value)

	iter := table.table.rangeFrom([]byte("abc333"))

	// Verify that iterator starts from index 2 which contains key abc333
	for i := 2; i < len(kvPairs); i++ {
		kv, ok := iter.Next().Get()
		assert.True(t, ok)
		assert.Equal(t, kvPairs[i].key, kv.key)
		assert.Equal(t, kvPairs[i].value, kv.value)
	}
}

func TestMemtableRangeFromNonExistingKey(t *testing.T) {
	kvPairs := []kv{
		{[]byte("abc111"), []byte("value1")},
		{[]byte("abc222"), []byte("value2")},
		{[]byte("abc333"), []byte("value3")},
		{[]byte("abc444"), []byte("value4")},
		{[]byte("abc555"), []byte("value5")},
	}

	table := newWritableKVTable()

	// Put keys in random order
	table.put(kvPairs[2].key, kvPairs[2].value)
	table.put(kvPairs[0].key, kvPairs[0].value)
	table.put(kvPairs[4].key, kvPairs[4].value)
	table.put(kvPairs[3].key, kvPairs[3].value)
	table.put(kvPairs[1].key, kvPairs[1].value)

	iter := table.table.rangeFrom([]byte("abc345"))

	// Verify that iterator starts from index 3 which contains key abc444
	for i := 3; i < len(kvPairs); i++ {
		kv, ok := iter.Next().Get()
		assert.True(t, ok)
		assert.Equal(t, kvPairs[i].key, kv.key)
		assert.Equal(t, kvPairs[i].value, kv.value)
	}
}

func TestMemtableIterDelete(t *testing.T) {
	table := newWritableKVTable()

	table.put([]byte("abc333"), []byte("value3"))
	iter := table.table.iter()
	assert.True(t, iter.Next().IsPresent())

	table.delete([]byte("abc333"))
	iter = table.table.iter()
	assert.False(t, iter.Next().IsPresent())
}

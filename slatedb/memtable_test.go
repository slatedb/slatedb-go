package slatedb

import (
	"github.com/naveen246/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemtableIter(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("abc111"), Value: []byte("value1")},
		{Key: []byte("abc222"), Value: []byte("value2")},
		{Key: []byte("abc333"), Value: []byte("value3")},
		{Key: []byte("abc444"), Value: []byte("value4")},
		{Key: []byte("abc555"), Value: []byte("value5")},
	}

	table := newWritableKVTable()

	// Put keys in random order
	table.put(kvPairs[2].Key, kvPairs[2].Value)
	table.put(kvPairs[0].Key, kvPairs[0].Value)
	table.put(kvPairs[4].Key, kvPairs[4].Value)
	table.put(kvPairs[3].Key, kvPairs[3].Value)
	table.put(kvPairs[1].Key, kvPairs[1].Value)

	iter := table.table.iter()

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

func TestMemtableRangeFromExistingKey(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("abc111"), Value: []byte("value1")},
		{Key: []byte("abc222"), Value: []byte("value2")},
		{Key: []byte("abc333"), Value: []byte("value3")},
		{Key: []byte("abc444"), Value: []byte("value4")},
		{Key: []byte("abc555"), Value: []byte("value5")},
	}

	table := newWritableKVTable()

	// Put keys in random order
	table.put(kvPairs[2].Key, kvPairs[2].Value)
	table.put(kvPairs[0].Key, kvPairs[0].Value)
	table.put(kvPairs[4].Key, kvPairs[4].Value)
	table.put(kvPairs[3].Key, kvPairs[3].Value)
	table.put(kvPairs[1].Key, kvPairs[1].Value)

	iter := table.table.rangeFrom([]byte("abc333"))

	// Verify that iterator starts from index 2 which contains key abc333
	for i := 2; i < len(kvPairs); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		kv, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, kvPairs[i].Key, kv.Key)
		assert.Equal(t, kvPairs[i].Value, kv.Value)
	}
}

func TestMemtableRangeFromNonExistingKey(t *testing.T) {
	kvPairs := []common.KV{
		{Key: []byte("abc111"), Value: []byte("value1")},
		{Key: []byte("abc222"), Value: []byte("value2")},
		{Key: []byte("abc333"), Value: []byte("value3")},
		{Key: []byte("abc444"), Value: []byte("value4")},
		{Key: []byte("abc555"), Value: []byte("value5")},
	}

	table := newWritableKVTable()

	// Put keys in random order
	table.put(kvPairs[2].Key, kvPairs[2].Value)
	table.put(kvPairs[0].Key, kvPairs[0].Value)
	table.put(kvPairs[4].Key, kvPairs[4].Value)
	table.put(kvPairs[3].Key, kvPairs[3].Value)
	table.put(kvPairs[1].Key, kvPairs[1].Value)

	iter := table.table.rangeFrom([]byte("abc345"))

	// Verify that iterator starts from index 3 which contains key abc444
	for i := 3; i < len(kvPairs); i++ {
		next, err := iter.Next()
		assert.NoError(t, err)
		kv, ok := next.Get()
		assert.True(t, ok)
		assert.Equal(t, kvPairs[i].Key, kv.Key)
		assert.Equal(t, kvPairs[i].Value, kv.Value)
	}
}

func TestMemtableIterDelete(t *testing.T) {
	table := newWritableKVTable()

	table.put([]byte("abc333"), []byte("value3"))
	next, err := table.table.iter().Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())

	table.delete([]byte("abc333"))
	next, err = table.table.iter().Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

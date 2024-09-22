package slatedb

import (
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"testing"
)

func TestOneSstSRIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(4096, 3, CompressionNone)
	tableStore := newTableStore(bucket, format, "")

	builder := tableStore.tableBuilder()
	builder.add([]byte("key1"), mo.Some([]byte("value1")))
	builder.add([]byte("key2"), mo.Some([]byte("value2")))
	builder.add([]byte("key3"), mo.Some([]byte("value3")))

	encodedSST, err := builder.build()
	assert.NoError(t, err)
	sstHandle, err := tableStore.writeSST(newSSTableIDCompacted(ulid.Make()), encodedSST)
	assert.NoError(t, err)

	sr := SortedRun{0, []SSTableHandle{*sstHandle}}
	iter := newSortedRunIterator(sr, tableStore, 1, 1)
	assertIterNext(t, iter, []byte("key1"), []byte("value1"))
	assertIterNext(t, iter, []byte("key2"), []byte("value2"))
	assertIterNext(t, iter, []byte("key3"), []byte("value3"))

	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.True(t, kv.IsAbsent())
}

func TestManySstSRIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(4096, 3, CompressionNone)
	tableStore := newTableStore(bucket, format, "")

	builder := tableStore.tableBuilder()
	builder.add([]byte("key1"), mo.Some([]byte("value1")))
	builder.add([]byte("key2"), mo.Some([]byte("value2")))

	encodedSST, err := builder.build()
	assert.NoError(t, err)
	sstHandle, err := tableStore.writeSST(newSSTableIDCompacted(ulid.Make()), encodedSST)
	assert.NoError(t, err)

	builder = tableStore.tableBuilder()
	builder.add([]byte("key3"), mo.Some([]byte("value3")))

	encodedSST, err = builder.build()
	assert.NoError(t, err)
	sstHandle2, err := tableStore.writeSST(newSSTableIDCompacted(ulid.Make()), encodedSST)
	assert.NoError(t, err)

	sr := SortedRun{0, []SSTableHandle{*sstHandle, *sstHandle2}}
	iter := newSortedRunIterator(sr, tableStore, 1, 1)
	assertIterNext(t, iter, []byte("key1"), []byte("value1"))
	assertIterNext(t, iter, []byte("key2"), []byte("value2"))
	assertIterNext(t, iter, []byte("key3"), []byte("value3"))

	kv, err := iter.Next()
	assert.NoError(t, err)
	assert.True(t, kv.IsAbsent())
}

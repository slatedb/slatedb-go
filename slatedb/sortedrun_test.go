package slatedb

import (
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"testing"
)

func buildSRWithSSTs(
	n uint64,
	keysPerSST uint64,
	tableStore *TableStore,
	keyGen common.OrderedBytesGenerator,
	valGen common.OrderedBytesGenerator,
) SortedRun {

	sstList := make([]SSTableHandle, 0, n)
	for i := uint64(0); i < n; i++ {
		writer := tableStore.tableWriter(newSSTableIDCompacted(ulid.Make()))
		for j := uint64(0); j < keysPerSST; j++ {
			writer.add(keyGen.Next(), mo.Some(valGen.Next()))
		}

		sst, _ := writer.close()
		sstList = append(sstList, *sst)
	}

	return SortedRun{0, sstList}
}

func TestOneSstSRIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.minFilterKeys = 3
	tableStore := newTableStore(bucket, format, "")

	builder := tableStore.tableBuilder()
	builder.Add([]byte("key1"), mo.Some([]byte("value1")))
	builder.Add([]byte("key2"), mo.Some([]byte("value2")))
	builder.Add([]byte("key3"), mo.Some([]byte("value3")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	sstHandle, err := tableStore.writeSST(newSSTableIDCompacted(ulid.Make()), encodedSST)
	assert.NoError(t, err)

	sr := SortedRun{0, []SSTableHandle{*sstHandle}}
	iterator, err := newSortedRunIterator(sr, tableStore, 1, 1)
	assert.NoError(t, err)
	assert2.Next(t, iterator, []byte("key1"), []byte("value1"))
	assert2.Next(t, iterator, []byte("key2"), []byte("value2"))
	assert2.Next(t, iterator, []byte("key3"), []byte("value3"))

	kv, ok := iterator.Next()
	assert.False(t, ok)
	assert.Equal(t, common.KV{}, kv)
}

func TestManySstSRIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.minFilterKeys = 3
	tableStore := newTableStore(bucket, format, "")

	builder := tableStore.tableBuilder()
	builder.Add([]byte("key1"), mo.Some([]byte("value1")))
	builder.Add([]byte("key2"), mo.Some([]byte("value2")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	sstHandle, err := tableStore.writeSST(newSSTableIDCompacted(ulid.Make()), encodedSST)
	assert.NoError(t, err)

	builder = tableStore.tableBuilder()
	builder.Add([]byte("key3"), mo.Some([]byte("value3")))

	encodedSST, err = builder.Build()
	assert.NoError(t, err)
	sstHandle2, err := tableStore.writeSST(newSSTableIDCompacted(ulid.Make()), encodedSST)
	assert.NoError(t, err)

	sr := SortedRun{0, []SSTableHandle{*sstHandle, *sstHandle2}}
	iterator, err := newSortedRunIterator(sr, tableStore, 1, 1)
	assert.NoError(t, err)
	assert2.Next(t, iterator, []byte("key1"), []byte("value1"))
	assert2.Next(t, iterator, []byte("key2"), []byte("value2"))
	assert2.Next(t, iterator, []byte("key3"), []byte("value3"))

	kv, ok := iterator.Next()
	assert.False(t, ok)
	assert.Equal(t, common.KV{}, kv)
}

func TestSRIterFromKey(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.minFilterKeys = 3
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))
	testCaseKeyGen := keyGen.Clone()

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	testCaseValGen := valGen.Clone()

	sr := buildSRWithSSTs(3, 10, tableStore, keyGen, valGen)

	for i := 0; i < 30; i++ {
		expectedKeyGen := testCaseKeyGen.Clone()
		expectedValGen := testCaseValGen.Clone()
		fromKey := testCaseKeyGen.Next()
		testCaseValGen.Next()

		kvIter, err := newSortedRunIteratorFromKey(sr, fromKey, tableStore, 1, 1)
		assert.NoError(t, err)

		for j := 0; j < 30-i; j++ {
			assert2.Next(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next())
		}
		next, ok := kvIter.Next()
		assert.False(t, ok)
		assert.Equal(t, common.KV{}, next)
	}
}

func TestSRIterFromKeyLowerThanRange(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.minFilterKeys = 3
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))
	expectedKeyGen := keyGen.Clone()

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	expectedValGen := valGen.Clone()

	sr := buildSRWithSSTs(3, 10, tableStore, keyGen, valGen)
	kvIter, err := newSortedRunIteratorFromKey(sr, []byte("aaaaaaaaaa"), tableStore, 1, 1)
	assert.NoError(t, err)

	for j := 0; j < 30; j++ {
		assert2.Next(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next())
	}
	next, ok := kvIter.Next()
	assert.False(t, ok)
	assert.Equal(t, common.KV{}, next)
}

func TestSRIterFromKeyHigherThanRange(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.minFilterKeys = 3
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))

	sr := buildSRWithSSTs(3, 10, tableStore, keyGen, valGen)
	kvIter, err := newSortedRunIteratorFromKey(sr, []byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), tableStore, 1, 1)
	assert.NoError(t, err)
	next, ok := kvIter.Next()
	assert.False(t, ok)
	assert.Equal(t, common.KV{}, next)
}

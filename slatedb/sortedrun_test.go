package slatedb

import (
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"testing"
)

func buildSRWithSSTs(
	n uint64,
	keysPerSST uint64,
	tableStore *TableStore,
	keyGen common.OrderedBytesGenerator,
	valGen common.OrderedBytesGenerator,
) (SortedRun, error) {

	sstList := make([]sstable.Handle, 0, n)
	for i := uint64(0); i < n; i++ {
		writer := tableStore.TableWriter(sstable.NewIDCompacted(ulid.Make()))
		for j := uint64(0); j < keysPerSST; j++ {
			if err := writer.Add(keyGen.Next(), mo.Some(valGen.Next())); err != nil {
				return SortedRun{}, err
			}
		}

		sst, _ := writer.Close()
		sstList = append(sstList, *sst)
	}

	return SortedRun{0, sstList}, nil
}

func TestOneSstSRIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := NewTableStore(bucket, conf, "")

	builder := tableStore.TableBuilder()
	require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
	require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))
	require.NoError(t, builder.AddValue([]byte("key3"), []byte("value3")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	sstHandle, err := tableStore.WriteSST(sstable.NewIDCompacted(ulid.Make()), encodedSST)
	assert.NoError(t, err)

	sr := SortedRun{0, []sstable.Handle{*sstHandle}}
	iterator, err := newSortedRunIterator(sr, tableStore, 1, 1)
	assert.NoError(t, err)
	assert2.Next(t, iterator, []byte("key1"), []byte("value1"))
	assert2.Next(t, iterator, []byte("key2"), []byte("value2"))
	assert2.Next(t, iterator, []byte("key3"), []byte("value3"))

	kv, ok := iterator.Next()
	assert.False(t, ok)
	assert.Equal(t, types.KeyValue{}, kv)
}

func TestManySstSRIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := sstable.DefaultConfig()
	format.MinFilterKeys = 3
	tableStore := NewTableStore(bucket, format, "")

	builder := tableStore.TableBuilder()
	require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
	require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	sstHandle, err := tableStore.WriteSST(sstable.NewIDCompacted(ulid.Make()), encodedSST)
	require.NoError(t, err)

	builder = tableStore.TableBuilder()
	err = builder.AddValue([]byte("key3"), []byte("value3"))
	require.NoError(t, err)

	encodedSST, err = builder.Build()
	require.NoError(t, err)
	sstHandle2, err := tableStore.WriteSST(sstable.NewIDCompacted(ulid.Make()), encodedSST)
	require.NoError(t, err)

	sr := SortedRun{0, []sstable.Handle{*sstHandle, *sstHandle2}}
	iterator, err := newSortedRunIterator(sr, tableStore, 1, 1)
	assert.NoError(t, err)
	assert2.Next(t, iterator, []byte("key1"), []byte("value1"))
	assert2.Next(t, iterator, []byte("key2"), []byte("value2"))
	assert2.Next(t, iterator, []byte("key3"), []byte("value3"))

	kv, ok := iterator.Next()
	assert.False(t, ok)
	assert.Equal(t, types.KeyValue{}, kv)
}

func TestSRIterFromKey(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := NewTableStore(bucket, conf, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))
	testCaseKeyGen := keyGen.Clone()

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	testCaseValGen := valGen.Clone()

	sr, err := buildSRWithSSTs(3, 10, tableStore, keyGen, valGen)
	require.NoError(t, err)

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
		assert.Equal(t, types.KeyValue{}, next)
	}
}

func TestSRIterFromKeyLowerThanRange(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := NewTableStore(bucket, conf, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))
	expectedKeyGen := keyGen.Clone()

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	expectedValGen := valGen.Clone()

	sr, err := buildSRWithSSTs(3, 10, tableStore, keyGen, valGen)
	require.NoError(t, err)

	kvIter, err := newSortedRunIteratorFromKey(sr, []byte("aaaaaaaaaa"), tableStore, 1, 1)
	assert.NoError(t, err)

	for j := 0; j < 30; j++ {
		assert2.Next(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next())
	}
	next, ok := kvIter.Next()
	assert.False(t, ok)
	assert.Equal(t, types.KeyValue{}, next)
}

func TestSRIterFromKeyHigherThanRange(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := NewTableStore(bucket, conf, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))

	sr, err := buildSRWithSSTs(3, 10, tableStore, keyGen, valGen)
	require.NoError(t, err)

	kvIter, err := newSortedRunIteratorFromKey(sr, []byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), tableStore, 1, 1)
	assert.NoError(t, err)
	next, ok := kvIter.Next()
	assert.False(t, ok)
	assert.Equal(t, types.KeyValue{}, next)
}

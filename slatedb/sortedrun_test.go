package slatedb

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/compacted"
	"github.com/slatedb/slatedb-go/slatedb/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func buildSRWithSSTs(
	n uint64,
	keysPerSST uint64,
	tableStore *store.TableStore,
	keyGen common.OrderedBytesGenerator,
	valGen common.OrderedBytesGenerator,
) (compacted.SortedRun, error) {

	sstList := make([]sstable.Handle, 0, n)
	for i := uint64(0); i < n; i++ {
		writer := tableStore.TableWriter(sstable.NewIDCompacted(ulid.Make()))
		for j := uint64(0); j < keysPerSST; j++ {
			if err := writer.Add(keyGen.Next(), mo.Some(valGen.Next())); err != nil {
				return compacted.SortedRun{}, err
			}
		}

		sst, _ := writer.Close(context.Background())
		sstList = append(sstList, *sst)
	}

	return compacted.SortedRun{ID: 0, SSTList: sstList}, nil
}

func TestOneSstSRIter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := store.NewTableStore(bucket, conf, "")

	builder := tableStore.TableBuilder()
	require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
	require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))
	require.NoError(t, builder.AddValue([]byte("key3"), []byte("value3")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	sstHandle, err := tableStore.WriteSST(ctx, sstable.NewIDCompacted(ulid.Make()), encodedSST)
	assert.NoError(t, err)

	sr := compacted.SortedRun{ID: 0, SSTList: []sstable.Handle{*sstHandle}}
	iterator, err := compacted.NewSortedRunIterator(ctx, sr, tableStore)
	assert.NoError(t, err)
	assert2.Next(t, iterator, []byte("key1"), []byte("value1"))
	assert2.Next(t, iterator, []byte("key2"), []byte("value2"))
	assert2.Next(t, iterator, []byte("key3"), []byte("value3"))

	next, ok := iterator.NextEntry(context.Background())
	assert.False(t, ok)
	assert.Equal(t, types.RowEntry{}, next)
}

func TestManySstSRIter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	bucket := objstore.NewInMemBucket()
	format := sstable.DefaultConfig()
	format.MinFilterKeys = 3
	tableStore := store.NewTableStore(bucket, format, "")

	builder := tableStore.TableBuilder()
	require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
	require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	sstHandle, err := tableStore.WriteSST(ctx, sstable.NewIDCompacted(ulid.Make()), encodedSST)
	require.NoError(t, err)

	builder = tableStore.TableBuilder()
	err = builder.AddValue([]byte("key3"), []byte("value3"))
	require.NoError(t, err)

	encodedSST, err = builder.Build()
	require.NoError(t, err)
	sstHandle2, err := tableStore.WriteSST(ctx, sstable.NewIDCompacted(ulid.Make()), encodedSST)
	require.NoError(t, err)

	sr := compacted.SortedRun{ID: 0, SSTList: []sstable.Handle{*sstHandle, *sstHandle2}}
	iterator, err := compacted.NewSortedRunIterator(ctx, sr, tableStore)
	assert.NoError(t, err)
	assert2.Next(t, iterator, []byte("key1"), []byte("value1"))
	assert2.Next(t, iterator, []byte("key2"), []byte("value2"))
	assert2.Next(t, iterator, []byte("key3"), []byte("value3"))

	next, ok := iterator.NextEntry(context.Background())
	assert.False(t, ok)
	assert.Equal(t, types.RowEntry{}, next)
}

func TestSRIterFromKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := store.NewTableStore(bucket, conf, "")

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

		kvIter, err := compacted.NewSortedRunIteratorFromKey(ctx, sr, fromKey, tableStore)
		assert.NoError(t, err)

		for j := 0; j < 30-i; j++ {
			assert2.Next(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next())
		}
		next, ok := kvIter.NextEntry(context.Background())
		assert.False(t, ok)
		assert.Equal(t, types.RowEntry{}, next)
	}
}

func TestSRIterFromKeyLowerThanRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := store.NewTableStore(bucket, conf, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))
	expectedKeyGen := keyGen.Clone()

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	expectedValGen := valGen.Clone()

	sr, err := buildSRWithSSTs(3, 10, tableStore, keyGen, valGen)
	require.NoError(t, err)

	kvIter, err := compacted.NewSortedRunIteratorFromKey(ctx, sr, []byte("aaaaaaaaaa"), tableStore)
	assert.NoError(t, err)

	for j := 0; j < 30; j++ {
		assert2.Next(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next())
	}
	next, ok := kvIter.NextEntry(context.Background())
	assert.False(t, ok)
	assert.Equal(t, types.RowEntry{}, next)
}

func TestSRIterFromKeyHigherThanRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := store.NewTableStore(bucket, conf, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))

	sr, err := buildSRWithSSTs(3, 10, tableStore, keyGen, valGen)
	require.NoError(t, err)

	kvIter, err := compacted.NewSortedRunIteratorFromKey(ctx, sr, []byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), tableStore)
	assert.NoError(t, err)
	next, ok := kvIter.NextEntry(context.Background())
	assert.False(t, ok)
	assert.Equal(t, types.RowEntry{}, next)
}

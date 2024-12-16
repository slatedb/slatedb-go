package slatedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/sstable/bloom"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"strconv"
	"testing"
)

type BytesBlob struct {
	data []byte
}

func (b BytesBlob) Len() (int, error) {
	return len(b.data), nil
}

func (b BytesBlob) ReadRange(r common.Range) ([]byte, error) {
	return b.data[r.Start:r.End], nil
}

func (b BytesBlob) Read() ([]byte, error) {
	return b.data, nil
}

func nextBlockToIter(t *testing.T, builder *sstable.Builder, codec compress.Codec) *block.Iterator {
	blockBytes, ok := builder.NextBlock().Get()
	common.AssertTrue(ok, "Block should not be empty")
	var decoded block.Block

	require.NoError(t, block.Decode(&decoded, blockBytes, codec))
	return block.NewIterator(&decoded)
}

func buildSSTWithNBlocks(
	n uint64,
	tableStore *TableStore,
	keyGen common.OrderedBytesGenerator,
	valGen common.OrderedBytesGenerator,
) (*sstable.Handle, int, error) {
	writer := tableStore.TableWriter(sstable.NewIDWal(0))
	nKeys := 0
	for writer.blocksWritten < n {
		if err := writer.Add(keyGen.Next(), mo.Some(valGen.Next())); err != nil {
			return nil, 0, err
		}
		nKeys += 1
	}
	sst, _ := writer.Close()
	return sst, nKeys, nil
}

func TestBuilderShouldMakeBlocksAvailable(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.BlockSize = 32
	tableStore := NewTableStore(bucket, conf, "")
	builder := tableStore.TableBuilder()
	require.NoError(t, builder.AddValue([]byte("aaaaaaaa"), []byte("11111111")))
	require.NoError(t, builder.AddValue([]byte("bbbbbbbb"), []byte("22222222")))
	require.NoError(t, builder.AddValue([]byte("cccccccc"), []byte("33333333")))

	iterator := nextBlockToIter(t, builder, conf.Compression)
	assert2.NextEntry(t, iterator, []byte("aaaaaaaa"), []byte("11111111"))
	_, ok := iterator.NextEntry()
	assert.False(t, ok)

	iterator = nextBlockToIter(t, builder, conf.Compression)
	assert2.NextEntry(t, iterator, []byte("bbbbbbbb"), []byte("22222222"))
	_, ok = iterator.NextEntry()
	assert.False(t, ok)

	assert.True(t, builder.NextBlock().IsAbsent())
	require.NoError(t, builder.AddValue([]byte("dddddddd"), []byte("44444444")))

	iterator = nextBlockToIter(t, builder, conf.Compression)
	assert2.NextEntry(t, iterator, []byte("cccccccc"), []byte("33333333"))
	_, ok = iterator.NextEntry()
	assert.False(t, ok)

	assert.True(t, builder.NextBlock().IsAbsent())
}

func TestBuilderShouldReturnUnconsumedBlocks(t *testing.T) {
	kvList := []types.KeyValue{
		{Key: []byte("aaaaaaaa"), Value: []byte("11111111")},
		{Key: []byte("bbbbbbbb"), Value: []byte("22222222")},
		{Key: []byte("cccccccc"), Value: []byte("33333333")},
	}

	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.BlockSize = 32
	tableStore := NewTableStore(bucket, conf, "")
	builder := tableStore.TableBuilder()
	for _, kv := range kvList {
		require.NoError(t, builder.AddValue(kv.Key, kv.Value))
	}

	firstBlock, ok := builder.NextBlock().Get()
	assert.True(t, ok)
	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	rawSST := firstBlock
	blockCount := encodedSST.Blocks.Len()
	assert.Equal(t, 2, blockCount)

	for i := 0; i < blockCount; i++ {
		rawSST = append(rawSST, encodedSST.Blocks.PopFront()...)
	}

	index, err := sstable.ReadIndexRaw(encodedSST.Info, rawSST)
	assert.NoError(t, err)

	for i, kv := range kvList {
		blk, err := sstable.ReadBlockRaw(encodedSST.Info, index, uint64(i), rawSST)
		assert.NoError(t, err)
		iterator := block.NewIterator(blk)
		assert2.NextEntry(t, iterator, kv.Key, kv.Value)
		_, ok := iterator.NextEntry()
		assert.False(t, ok)
	}
}

func TestSSTable(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	tableStore := NewTableStore(bucket, conf, "")
	builder := tableStore.TableBuilder()

	require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
	require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	encodedInfo := encodedSST.Info

	// write sst and validate that the handle returned has the correct content.
	sstHandle, err := tableStore.WriteSST(sstable.NewIDWal(0), encodedSST)
	assert.NoError(t, err)
	assert.Equal(t, encodedInfo, sstHandle.Info)
	firstKey := sstHandle.Info.FirstKey
	assert.NotNil(t, firstKey)
	assert.True(t, bytes.Equal(firstKey, []byte("key1")))

	// construct sst info from the raw bytes and validate that it matches the original info.
	sstHandleFromStore, err := tableStore.OpenSST(sstable.NewIDWal(0))
	assert.NoError(t, err)
	assert.Equal(t, encodedInfo, sstHandleFromStore.Info)

	sstInfoFromStore := sstHandleFromStore.Info
	index, err := tableStore.ReadIndex(sstHandleFromStore)
	assert.NoError(t, err)
	assert.Equal(t, 1, index.BlockMetaLength())
	firstKey = sstInfoFromStore.FirstKey
	assert.NotNil(t, firstKey)
	assert.True(t, bytes.Equal(firstKey, []byte("key1")))
	firstKey = index.BlockMeta()[0].FirstKey
	assert.True(t, bytes.Equal(firstKey, []byte("key1")))
}

func TestSSTableNoFilter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := NewTableStore(bucket, conf, "")
	builder := tableStore.TableBuilder()

	require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
	require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	encodedInfo := encodedSST.Info

	_, err = tableStore.WriteSST(sstable.NewIDWal(0), encodedSST)
	assert.NoError(t, err)
	sstHandle, err := tableStore.OpenSST(sstable.NewIDWal(0))
	assert.NoError(t, err)
	assert.Equal(t, encodedInfo, sstHandle.Info)
	assert.Equal(t, uint64(0), sstHandle.Info.FilterLen)
}

func TestSSTableBuildsFilterWithCorrectBitsPerKey(t *testing.T) {
	filterBits := []uint32{10, 20}
	for _, filterBitsPerKey := range filterBits {
		bucket := objstore.NewInMemBucket()
		conf := sstable.DefaultConfig()
		conf.FilterBitsPerKey = filterBitsPerKey
		tableStore := NewTableStore(bucket, conf, "")
		builder := tableStore.TableBuilder()
		for i := 0; i < 8; i++ {
			require.NoError(t, builder.AddValue([]byte(strconv.Itoa(i)), []byte("value")))
		}
		encodedSST, err := builder.Build()
		assert.NoError(t, err)
		filter, _ := encodedSST.Bloom.Get()
		// filters are encoded as a 2 byte number of probes followed by the filter
		// Since we have added 8 keys, the filter will have (8 * FilterBitsPerKey) bits or FilterBitsPerKey bytes
		assert.Equal(t, 2+int(filterBitsPerKey), len(bloom.Encode(filter)))
	}
}

func TestSSTableWithCompression(t *testing.T) {
	codecs := []compress.Codec{compress.CodecSnappy, compress.CodecZlib, compress.CodecLz4, compress.CodecZstd}
	for _, compression := range codecs {
		bucket := objstore.NewInMemBucket()
		conf := sstable.DefaultConfig()
		conf.Compression = compression
		tableStore := NewTableStore(bucket, conf, "")
		builder := tableStore.TableBuilder()

		require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
		require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))

		encodedSST, err := builder.Build()
		assert.NoError(t, err)
		encodedInfo := encodedSST.Info

		_, err = tableStore.WriteSST(sstable.NewIDWal(0), encodedSST)
		assert.NoError(t, err)
		sstHandle, err := tableStore.OpenSST(sstable.NewIDWal(0))
		assert.NoError(t, err)
		index, err := tableStore.ReadIndex(sstHandle)
		assert.NoError(t, err)

		assert.Equal(t, encodedInfo, sstHandle.Info)
		assert.Equal(t, 1, index.BlockMetaLength())
		firstKey := sstHandle.Info.FirstKey
		assert.NotNil(t, firstKey)
		assert.True(t, bytes.Equal(firstKey, []byte("key1")))
	}
}

func TestReadBlocks(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.BlockSize = 52
	conf.MinFilterKeys = 1
	tableStore := NewTableStore(bucket, conf, "")
	builder := tableStore.TableBuilder()

	require.NoError(t, builder.AddValue([]byte("aa"), []byte("11")))
	require.NoError(t, builder.AddValue([]byte("bb"), []byte("22")))
	require.NoError(t, builder.AddValue([]byte("cccccccccccccccccccc"), []byte("33333333333333333333")))
	require.NoError(t, builder.AddValue([]byte("dddddddddddddddddddd"), []byte("44444444444444444444")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	encodedInfo := encodedSST.Info

	data := make([]byte, 0)
	for i := 0; i < encodedSST.Blocks.Len(); i++ {
		data = append(data, encodedSST.Blocks.At(i)...)
	}

	blob := BytesBlob{data}
	index, err := sstable.ReadIndexRaw(encodedInfo, data)
	assert.NoError(t, err)
	blocks, err := sstable.ReadBlocks(encodedInfo, index, common.Range{Start: 0, End: 2}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(blocks))

	iterator := block.NewIterator(&blocks[0])
	assert2.NextEntry(t, iterator, []byte("aa"), []byte("11"))
	assert2.NextEntry(t, iterator, []byte("bb"), []byte("22"))
	_, ok := iterator.NextEntry()
	assert.False(t, ok)

	iterator = block.NewIterator(&blocks[1])
	assert2.NextEntry(t, iterator, []byte("cccccccccccccccccccc"), []byte("33333333333333333333"))
	_, ok = iterator.NextEntry()
	assert.False(t, ok)
}

func TestReadAllBlocks(t *testing.T) {
	// Force the creation of multiple blocks
	blockSize := block.V0EstimateBlockSize([]types.KeyValue{
		{Key: []byte("aa"), Value: []byte("11")},
		{Key: []byte("bb"), Value: []byte("22")},
	})

	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.BlockSize = blockSize
	conf.MinFilterKeys = 1
	tableStore := NewTableStore(bucket, conf, "")
	builder := tableStore.TableBuilder()

	require.NoError(t, builder.AddValue([]byte("aa"), []byte("11")))
	require.NoError(t, builder.AddValue([]byte("bb"), []byte("22")))
	require.NoError(t, builder.AddValue([]byte("cccccccccccccccccccc"), []byte("33333333333333333333")))
	require.NoError(t, builder.AddValue([]byte("dddddddddddddddddddd"), []byte("44444444444444444444")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	encodedInfo := encodedSST.Info

	data := make([]byte, 0)
	for i := 0; i < encodedSST.Blocks.Len(); i++ {
		data = append(data, encodedSST.Blocks.At(i)...)
	}

	blob := BytesBlob{data}
	index, err := sstable.ReadIndexRaw(encodedInfo, data)
	assert.NoError(t, err)
	blocks, err := sstable.ReadBlocks(encodedInfo, index, common.Range{Start: 0, End: 3}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(blocks))

	iterator := block.NewIterator(&blocks[0])
	assert2.NextEntry(t, iterator, []byte("aa"), []byte("11"))
	assert2.NextEntry(t, iterator, []byte("bb"), []byte("22"))
	_, ok := iterator.NextEntry()
	assert.False(t, ok)

	iterator = block.NewIterator(&blocks[1])
	assert2.NextEntry(t, iterator, []byte("cccccccccccccccccccc"), []byte("33333333333333333333"))
	_, ok = iterator.NextEntry()
	assert.False(t, ok)

	iterator = block.NewIterator(&blocks[2])
	assert2.NextEntry(t, iterator, []byte("dddddddddddddddddddd"), []byte("44444444444444444444"))
	_, ok = iterator.NextEntry()
	assert.False(t, ok)
}

// Iterator tests

func TestOneBlockSSTIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := NewTableStore(bucket, conf, "")
	builder := tableStore.TableBuilder()

	require.NoError(t, builder.AddValue([]byte("key1"), []byte("value1")))
	require.NoError(t, builder.AddValue([]byte("key2"), []byte("value2")))
	require.NoError(t, builder.AddValue([]byte("key3"), []byte("value3")))
	require.NoError(t, builder.AddValue([]byte("key4"), []byte("value4")))

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	_, err = tableStore.WriteSST(sstable.NewIDWal(0), encodedSST)
	require.NoError(t, err)
	sstHandle, err := tableStore.OpenSST(sstable.NewIDWal(0))
	assert.NoError(t, err)
	index, err := tableStore.ReadIndex(sstHandle)
	assert.NoError(t, err)
	assert.Equal(t, 1, index.BlockMetaLength())

	iterator, err := sstable.NewIterator(sstHandle, tableStore, 1, 1)
	assert.NoError(t, err)
	assert2.Next(t, iterator, []byte("key1"), []byte("value1"))
	assert2.Next(t, iterator, []byte("key2"), []byte("value2"))
	assert2.Next(t, iterator, []byte("key3"), []byte("value3"))
	assert2.Next(t, iterator, []byte("key4"), []byte("value4"))

	_, ok := iterator.Next()
	assert.False(t, ok)
}

func TestManyBlockSSTIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 3
	tableStore := NewTableStore(bucket, conf, "")
	builder := tableStore.TableBuilder()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, builder.AddValue(key, value))
	}

	encodedSST, err := builder.Build()
	assert.NoError(t, err)
	_, err = tableStore.WriteSST(sstable.NewIDWal(0), encodedSST)
	require.NoError(t, err)
	sstHandle, err := tableStore.OpenSST(sstable.NewIDWal(0))
	assert.NoError(t, err)
	index, err := tableStore.ReadIndex(sstHandle)
	require.NoError(t, err)
	require.NotNil(t, index)

	iterator, err := sstable.NewIterator(sstHandle, tableStore, 1, 1)
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if !assert2.Next(t, iterator, key, value) {
			t.FailNow()
		}
	}

	_, ok := iterator.Next()
	assert.False(t, ok)
}

func TestIterFromKey(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 1
	tableStore := NewTableStore(bucket, conf, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))
	testCaseKeyGen := keyGen.Clone()

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	testCaseValGen := valGen.Clone()

	sst, nKeys, err := buildSSTWithNBlocks(3, tableStore, keyGen, valGen)
	require.NoError(t, err)

	for i := 0; i < nKeys; i++ {
		expectedKeyGen := testCaseKeyGen.Clone()
		expectedValGen := testCaseValGen.Clone()
		fromKey := testCaseKeyGen.Next()
		testCaseValGen.Next()
		kvIter, err := sstable.NewIteratorAtKey(sst, fromKey, tableStore, 1, 1)
		assert.NoError(t, err)

		for j := 0; j < nKeys-i; j++ {
			if !assert2.Next(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next()) {
				t.FailNow()
			}
		}
		_, ok := kvIter.Next()
		assert.False(t, ok)
	}
}

func TestIterFromKeySmallerThanFirst(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 1
	tableStore := NewTableStore(bucket, conf, "")

	firstKey := []byte("bbbbbbbbbbbbbbbb")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('y'))
	expectedKeyGen := keyGen.Clone()

	firstVal := []byte("2222222222222222")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	expectedValGen := valGen.Clone()

	sst, nKeys, err := buildSSTWithNBlocks(2, tableStore, keyGen, valGen)
	require.NoError(t, err)

	kvIter, err := sstable.NewIteratorAtKey(sst, []byte("aaaaaaaaaaaaaaaa"), tableStore, 1, 1)
	assert.NoError(t, err)

	for i := 0; i < nKeys; i++ {
		assert2.Next(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next())
	}
	_, ok := kvIter.Next()
	assert.False(t, ok)
}

func TestIterFromKeyLargerThanLast(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.MinFilterKeys = 1
	tableStore := NewTableStore(bucket, conf, "")

	firstKey := []byte("bbbbbbbbbbbbbbbb")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('y'))

	firstVal := []byte("2222222222222222")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))

	sst, _, err := buildSSTWithNBlocks(2, tableStore, keyGen, valGen)
	require.NoError(t, err)
	kvIter, err := sstable.NewIteratorAtKey(sst, []byte("zzzzzzzzzzzzzzzz"), tableStore, 1, 1)
	assert.NoError(t, err)

	_, ok := kvIter.Next()
	assert.False(t, ok)
}

func TestShouldGenerateOrderedBytes(t *testing.T) {
	suffix := make([]byte, common.SizeOfUint32)
	binary.BigEndian.PutUint32(suffix, 3735928559)
	start := []byte{0, 0, 0}
	gen := common.NewOrderedBytesGenerator(suffix, start, 0, 2)
	expected := [][]byte{
		{0, 0, 0, 0xde, 0xad, 0xbe, 0xef},
		{0, 0, 1, 0xde, 0xad, 0xbe, 0xef},
		{0, 0, 2, 0xde, 0xad, 0xbe, 0xef},
		{0, 1, 0, 0xde, 0xad, 0xbe, 0xef},
		{0, 1, 1, 0xde, 0xad, 0xbe, 0xef},
		{0, 1, 2, 0xde, 0xad, 0xbe, 0xef},
		{0, 2, 0, 0xde, 0xad, 0xbe, 0xef},
		{0, 2, 1, 0xde, 0xad, 0xbe, 0xef},
		{0, 2, 2, 0xde, 0xad, 0xbe, 0xef},
		{1, 0, 0, 0xde, 0xad, 0xbe, 0xef},
		{1, 0, 1, 0xde, 0xad, 0xbe, 0xef},
		{1, 0, 2, 0xde, 0xad, 0xbe, 0xef},
		{1, 1, 0, 0xde, 0xad, 0xbe, 0xef},
		{1, 1, 1, 0xde, 0xad, 0xbe, 0xef},
		{1, 1, 2, 0xde, 0xad, 0xbe, 0xef},
		{1, 2, 0, 0xde, 0xad, 0xbe, 0xef},
		{1, 2, 1, 0xde, 0xad, 0xbe, 0xef},
		{1, 2, 2, 0xde, 0xad, 0xbe, 0xef},
		{2, 0, 0, 0xde, 0xad, 0xbe, 0xef},
		{2, 0, 1, 0xde, 0xad, 0xbe, 0xef},
		{2, 0, 2, 0xde, 0xad, 0xbe, 0xef},
		{2, 1, 0, 0xde, 0xad, 0xbe, 0xef},
		{2, 1, 1, 0xde, 0xad, 0xbe, 0xef},
		{2, 1, 2, 0xde, 0xad, 0xbe, 0xef},
		{2, 2, 0, 0xde, 0xad, 0xbe, 0xef},
		{2, 2, 1, 0xde, 0xad, 0xbe, 0xef},
	}

	for _, b := range expected {
		assert.Equal(t, b, gen.Next())
	}
}

func TestSSTWriter(t *testing.T) {
	// Force key values into separate blocks
	blockSize := block.V0EstimateBlockSize([]types.KeyValue{
		{Key: []byte("aaaaaaaaaaaaaaaa"), Value: []byte("1111111111111111")},
	})

	bucket := objstore.NewInMemBucket()
	conf := sstable.DefaultConfig()
	conf.BlockSize = blockSize
	conf.FilterBitsPerKey = 1
	tableStore := NewTableStore(bucket, conf, "")
	sstID := sstable.NewIDCompacted(ulid.Make())

	writer := tableStore.TableWriter(sstID)
	require.NoError(t, writer.Add([]byte("aaaaaaaaaaaaaaaa"), mo.Some([]byte("1111111111111111"))))
	require.NoError(t, writer.Add([]byte("bbbbbbbbbbbbbbbb"), mo.Some([]byte("2222222222222222"))))
	require.NoError(t, writer.Add([]byte("cccccccccccccccc"), mo.None[[]byte]()))
	require.NoError(t, writer.Add([]byte("dddddddddddddddd"), mo.Some([]byte("4444444444444444"))))
	sst, err := writer.Close()
	assert.NoError(t, err)

	iterator, err := sstable.NewIterator(sst, tableStore, 1, 1)
	assert.NoError(t, err)
	assert2.NextEntry(t, iterator, []byte("aaaaaaaaaaaaaaaa"), []byte("1111111111111111"))
	assert2.NextEntry(t, iterator, []byte("bbbbbbbbbbbbbbbb"), []byte("2222222222222222"))
	assert2.NextEntry(t, iterator, []byte("cccccccccccccccc"), nil)
	assert2.NextEntry(t, iterator, []byte("dddddddddddddddd"), []byte("4444444444444444"))
	_, ok := iterator.NextEntry()
	assert.False(t, ok)
}

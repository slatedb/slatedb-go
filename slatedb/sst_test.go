package slatedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/iter"
	"github.com/stretchr/testify/assert"
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

func nextBlockToIter(builder *EncodedSSTableBuilder) *BlockIterator {
	blockBytes, ok := builder.nextBlock().Get()
	common.AssertTrue(ok, "Block should not be empty")
	block := decodeBytesToBlock(blockBytes[:len(blockBytes)-common.SizeOfUint32InBytes])
	return newBlockIteratorFromFirstKey(block)
}

func buildSSTWithNBlocks(
	n uint64,
	tableStore *TableStore,
	keyGen common.OrderedBytesGenerator,
	valGen common.OrderedBytesGenerator,
) (*SSTableHandle, int) {
	writer := tableStore.tableWriter(newSSTableIDWal(0))
	nKeys := 0
	for writer.blocksWritten < n {
		writer.add(keyGen.Next(), mo.Some(valGen.Next()))
		nKeys += 1
	}
	sst, _ := writer.close()
	return sst, nKeys
}

func TestBuilderShouldMakeBlocksAvailable(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.blockSize = 32
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()
	builder.add([]byte("aaaaaaaa"), mo.Some([]byte("11111111")))
	builder.add([]byte("bbbbbbbb"), mo.Some([]byte("22222222")))
	builder.add([]byte("cccccccc"), mo.Some([]byte("33333333")))

	iterator := nextBlockToIter(builder)
	iter.AssertIterNextEntry(t, iterator, []byte("aaaaaaaa"), []byte("11111111"))
	nextEntry, err := iterator.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	iterator = nextBlockToIter(builder)
	iter.AssertIterNextEntry(t, iterator, []byte("bbbbbbbb"), []byte("22222222"))
	nextEntry, err = iterator.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	assert.True(t, builder.nextBlock().IsAbsent())
	builder.add([]byte("dddddddd"), mo.Some([]byte("44444444")))

	iterator = nextBlockToIter(builder)
	iter.AssertIterNextEntry(t, iterator, []byte("cccccccc"), []byte("33333333"))
	nextEntry, err = iterator.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	assert.True(t, builder.nextBlock().IsAbsent())
}

func TestBuilderShouldReturnUnconsumedBlocks(t *testing.T) {
	kvList := []common.KV{
		{Key: []byte("aaaaaaaa"), Value: []byte("11111111")},
		{Key: []byte("bbbbbbbb"), Value: []byte("22222222")},
		{Key: []byte("cccccccc"), Value: []byte("33333333")},
	}

	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.blockSize = 32
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()
	for _, kv := range kvList {
		builder.add(kv.Key, mo.Some(kv.Value))
	}

	firstBlock, ok := builder.nextBlock().Get()
	assert.True(t, ok)
	encodedSST, err := builder.build()
	assert.NoError(t, err)
	rawSST := firstBlock
	blockCount := encodedSST.unconsumedBlocks.Len()
	assert.Equal(t, 2, blockCount)

	for i := 0; i < blockCount; i++ {
		rawSST = append(rawSST, encodedSST.unconsumedBlocks.PopFront()...)
	}

	index, err := format.readIndexRaw(encodedSST.sstInfo, rawSST)
	assert.NoError(t, err)

	for i, kv := range kvList {
		block, err := format.readBlockRaw(encodedSST.sstInfo, index, uint64(i), rawSST)
		assert.NoError(t, err)
		iterator := newBlockIteratorFromFirstKey(block)
		iter.AssertIterNextEntry(t, iterator, kv.Key, kv.Value)
		nextEntry, err := iterator.NextEntry()
		assert.NoError(t, err)
		assert.True(t, nextEntry.IsAbsent())
	}
}

func TestSSTable(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()

	builder.add([]byte("key1"), mo.Some([]byte("value1")))
	builder.add([]byte("key2"), mo.Some([]byte("value2")))

	encodedSST, err := builder.build()
	assert.NoError(t, err)
	encodedInfo := encodedSST.sstInfo

	// write sst and validate that the handle returned has the correct content.
	sstHandle, err := tableStore.writeSST(newSSTableIDWal(0), encodedSST)
	assert.NoError(t, err)
	assert.Equal(t, encodedInfo, sstHandle.info)
	firstKey, ok := sstHandle.info.firstKey.Get()
	assert.True(t, ok)
	assert.True(t, bytes.Equal(firstKey, []byte("key1")))

	// construct sst info from the raw bytes and validate that it matches the original info.
	sstHandleFromStore, err := tableStore.openSST(newSSTableIDWal(0))
	assert.NoError(t, err)
	assert.Equal(t, encodedInfo, sstHandleFromStore.info)

	sstInfoFromStore := sstHandleFromStore.info
	index, err := tableStore.readIndex(sstHandleFromStore)
	assert.NoError(t, err)
	assert.Equal(t, 1, index.ssTableIndex().BlockMetaLength())
	firstKey, ok = sstInfoFromStore.firstKey.Get()
	assert.True(t, ok)
	assert.True(t, bytes.Equal(firstKey, []byte("key1")))
	firstKey = index.ssTableIndex().UnPack().BlockMeta[0].FirstKey
	assert.True(t, bytes.Equal(firstKey, []byte("key1")))
}

func TestSSTableNoFilter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.minFilterKeys = 3
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()

	builder.add([]byte("key1"), mo.Some([]byte("value1")))
	builder.add([]byte("key2"), mo.Some([]byte("value2")))

	encodedSST, err := builder.build()
	assert.NoError(t, err)
	encodedInfo := encodedSST.sstInfo

	_, err = tableStore.writeSST(newSSTableIDWal(0), encodedSST)
	assert.NoError(t, err)
	sstHandle, err := tableStore.openSST(newSSTableIDWal(0))
	assert.NoError(t, err)
	assert.Equal(t, encodedInfo, sstHandle.info)
	assert.Equal(t, uint64(0), sstHandle.info.filterLen)
}

func TestSSTableBuildsFilterWithCorrectBitsPerKey(t *testing.T) {
	filterBits := []uint32{10, 20}
	for _, filterBitsPerKey := range filterBits {
		bucket := objstore.NewInMemBucket()
		format := defaultSSTableFormat()
		format.filterBitsPerKey = filterBitsPerKey
		tableStore := newTableStore(bucket, format, "")
		builder := tableStore.tableBuilder()
		for i := 0; i < 8; i++ {
			builder.add([]byte(strconv.Itoa(i)), mo.Some([]byte("value")))
		}
		encodedSST, err := builder.build()
		assert.NoError(t, err)
		filter, _ := encodedSST.filter.Get()
		// filters are encoded as a 2 byte number of probes followed by the filter
		// Since we have added 8 keys, the filter will have (8 * filterBitsPerKey) bits or filterBitsPerKey bytes
		assert.Equal(t, 2+int(filterBitsPerKey), len(filter.EncodeToBytes()))
	}
}

func TestSSTableWithCompression(t *testing.T) {
	codecs := []CompressionCodec{CompressionSnappy, CompressionZlib, CompressionLz4, CompressionZstd}
	for _, compression := range codecs {
		bucket := objstore.NewInMemBucket()
		format := defaultSSTableFormat()
		format.compressionCodec = compression
		tableStore := newTableStore(bucket, format, "")
		builder := tableStore.tableBuilder()

		builder.add([]byte("key1"), mo.Some([]byte("value1")))
		builder.add([]byte("key2"), mo.Some([]byte("value2")))

		encodedSST, err := builder.build()
		assert.NoError(t, err)
		encodedInfo := encodedSST.sstInfo

		_, err = tableStore.writeSST(newSSTableIDWal(0), encodedSST)
		assert.NoError(t, err)
		sstHandle, err := tableStore.openSST(newSSTableIDWal(0))
		assert.NoError(t, err)
		index, err := tableStore.readIndex(sstHandle)
		assert.NoError(t, err)

		assert.Equal(t, encodedInfo, sstHandle.info)
		assert.Equal(t, 1, index.ssTableIndex().BlockMetaLength())
		firstKey, ok := sstHandle.info.firstKey.Get()
		assert.True(t, ok)
		assert.True(t, bytes.Equal(firstKey, []byte("key1")))
	}
}

func TestReadBlocks(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.blockSize = 32
	format.minFilterKeys = 1
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()

	builder.add([]byte("aa"), mo.Some([]byte("11")))
	builder.add([]byte("bb"), mo.Some([]byte("22")))
	builder.add([]byte("cccccccccccccccccccc"), mo.Some([]byte("33333333333333333333")))
	builder.add([]byte("dddddddddddddddddddd"), mo.Some([]byte("44444444444444444444")))

	encodedSST, err := builder.build()
	assert.NoError(t, err)
	encodedInfo := encodedSST.sstInfo

	data := make([]byte, 0)
	for i := 0; i < encodedSST.unconsumedBlocks.Len(); i++ {
		data = append(data, encodedSST.unconsumedBlocks.At(i)...)
	}

	blob := BytesBlob{data}
	index, err := format.readIndexRaw(encodedInfo, data)
	assert.NoError(t, err)
	blocks, err := format.readBlocks(encodedInfo, index, common.Range{Start: 0, End: 2}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(blocks))

	iterator := newBlockIteratorFromFirstKey(&blocks[0])
	iter.AssertIterNextEntry(t, iterator, []byte("aa"), []byte("11"))
	iter.AssertIterNextEntry(t, iterator, []byte("bb"), []byte("22"))
	nextEntry, err := iterator.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	iterator = newBlockIteratorFromFirstKey(&blocks[1])
	iter.AssertIterNextEntry(t, iterator, []byte("cccccccccccccccccccc"), []byte("33333333333333333333"))
	nextEntry, err = iterator.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())
}

func TestReadAllBlocks(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.blockSize = 32
	format.minFilterKeys = 1
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()

	builder.add([]byte("aa"), mo.Some([]byte("11")))
	builder.add([]byte("bb"), mo.Some([]byte("22")))
	builder.add([]byte("cccccccccccccccccccc"), mo.Some([]byte("33333333333333333333")))
	builder.add([]byte("dddddddddddddddddddd"), mo.Some([]byte("44444444444444444444")))

	encodedSST, err := builder.build()
	assert.NoError(t, err)
	encodedInfo := encodedSST.sstInfo

	data := make([]byte, 0)
	for i := 0; i < encodedSST.unconsumedBlocks.Len(); i++ {
		data = append(data, encodedSST.unconsumedBlocks.At(i)...)
	}

	blob := BytesBlob{data}
	index, err := format.readIndexRaw(encodedInfo, data)
	assert.NoError(t, err)
	blocks, err := format.readBlocks(encodedInfo, index, common.Range{Start: 0, End: 3}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(blocks))

	iterator := newBlockIteratorFromFirstKey(&blocks[0])
	iter.AssertIterNextEntry(t, iterator, []byte("aa"), []byte("11"))
	iter.AssertIterNextEntry(t, iterator, []byte("bb"), []byte("22"))
	nextEntry, err := iterator.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	iterator = newBlockIteratorFromFirstKey(&blocks[1])
	iter.AssertIterNextEntry(t, iterator, []byte("cccccccccccccccccccc"), []byte("33333333333333333333"))
	nextEntry, err = iterator.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	iterator = newBlockIteratorFromFirstKey(&blocks[2])
	iter.AssertIterNextEntry(t, iterator, []byte("dddddddddddddddddddd"), []byte("44444444444444444444"))
	nextEntry, err = iterator.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())
}

// SSTIterator tests

func TestOneBlockSSTIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.minFilterKeys = 3
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()

	builder.add([]byte("key1"), mo.Some([]byte("value1")))
	builder.add([]byte("key2"), mo.Some([]byte("value2")))
	builder.add([]byte("key3"), mo.Some([]byte("value3")))
	builder.add([]byte("key4"), mo.Some([]byte("value4")))

	encodedSST, err := builder.build()
	assert.NoError(t, err)
	tableStore.writeSST(newSSTableIDWal(0), encodedSST)
	sstHandle, err := tableStore.openSST(newSSTableIDWal(0))
	assert.NoError(t, err)
	index, err := tableStore.readIndex(sstHandle)
	assert.NoError(t, err)
	assert.Equal(t, 1, index.ssTableIndex().BlockMetaLength())

	iterator, err := newSSTIterator(sstHandle, tableStore, 1, 1)
	assert.NoError(t, err)
	iter.AssertIterNext(t, iterator, []byte("key1"), []byte("value1"))
	iter.AssertIterNext(t, iterator, []byte("key2"), []byte("value2"))
	iter.AssertIterNext(t, iterator, []byte("key3"), []byte("value3"))
	iter.AssertIterNext(t, iterator, []byte("key4"), []byte("value4"))

	next, err := iterator.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestManyBlockSSTIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.minFilterKeys = 3
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		builder.add(key, mo.Some(value))
	}

	encodedSST, err := builder.build()
	assert.NoError(t, err)
	tableStore.writeSST(newSSTableIDWal(0), encodedSST)
	sstHandle, err := tableStore.openSST(newSSTableIDWal(0))
	assert.NoError(t, err)
	index, err := tableStore.readIndex(sstHandle)
	assert.Equal(t, 6, index.ssTableIndex().BlockMetaLength())

	iterator, err := newSSTIterator(sstHandle, tableStore, 1, 1)
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		iter.AssertIterNext(t, iterator, key, value)
	}

	next, err := iterator.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestIterFromKey(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.blockSize = 128
	format.minFilterKeys = 1
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))
	testCaseKeyGen := keyGen.Clone()

	firstVal := []byte("1111111111111111")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	testCaseValGen := valGen.Clone()

	sst, nKeys := buildSSTWithNBlocks(3, tableStore, keyGen, valGen)

	for i := 0; i < nKeys; i++ {
		expectedKeyGen := testCaseKeyGen.Clone()
		expectedValGen := testCaseValGen.Clone()
		fromKey := testCaseKeyGen.Next()
		testCaseValGen.Next()
		kvIter, err := newSSTIteratorFromKey(sst, fromKey, tableStore, 1, 1)
		assert.NoError(t, err)

		for j := 0; j < nKeys-i; j++ {
			iter.AssertIterNext(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next())
		}
		next, err := kvIter.Next()
		assert.NoError(t, err)
		assert.False(t, next.IsPresent())
	}
}

func TestIterFromKeySmallerThanFirst(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.blockSize = 128
	format.minFilterKeys = 1
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("bbbbbbbbbbbbbbbb")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('y'))
	expectedKeyGen := keyGen.Clone()

	firstVal := []byte("2222222222222222")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	expectedValGen := valGen.Clone()

	sst, nKeys := buildSSTWithNBlocks(2, tableStore, keyGen, valGen)
	kvIter, err := newSSTIteratorFromKey(sst, []byte("aaaaaaaaaaaaaaaa"), tableStore, 1, 1)
	assert.NoError(t, err)

	for i := 0; i < nKeys; i++ {
		iter.AssertIterNext(t, kvIter, expectedKeyGen.Next(), expectedValGen.Next())
	}
	next, err := kvIter.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestIterFromKeyLargerThanLast(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.blockSize = 128
	format.minFilterKeys = 1
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("bbbbbbbbbbbbbbbb")
	keyGen := common.NewOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('y'))

	firstVal := []byte("2222222222222222")
	valGen := common.NewOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))

	sst, _ := buildSSTWithNBlocks(2, tableStore, keyGen, valGen)
	kvIter, err := newSSTIteratorFromKey(sst, []byte("zzzzzzzzzzzzzzzz"), tableStore, 1, 1)
	assert.NoError(t, err)

	next, err := kvIter.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestShouldGenerateOrderedBytes(t *testing.T) {
	suffix := make([]byte, common.SizeOfUint32InBytes)
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

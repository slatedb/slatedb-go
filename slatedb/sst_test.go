package slatedb

import (
	"encoding/binary"
	"fmt"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"testing"
)

type BytesBlob struct {
	data []byte
}

func (b BytesBlob) len() (int, error) {
	return len(b.data), nil
}

func (b BytesBlob) readRange(r Range) ([]byte, error) {
	return b.data[r.start:r.end], nil
}

func (b BytesBlob) read() ([]byte, error) {
	return b.data, nil
}

func nextBlockToIter(builder *EncodedSSTableBuilder) *BlockIterator {
	blockBytes, ok := builder.nextBlock().Get()
	assertTrue(ok, "Block should not be empty")
	block := decodeBytesToBlock(blockBytes[:len(blockBytes)-SizeOfUint32InBytes])
	return newBlockIteratorFromFirstKey(block)
}

func buildSSTWithNBlocks(
	n uint64,
	tableStore *TableStore,
	keyGen OrderedBytesGenerator,
	valGen OrderedBytesGenerator,
) (*SSTableHandle, int) {
	writer := tableStore.tableWriter(newSSTableIDWal(0))
	nKeys := 0
	for writer.blocksWritten < n {
		writer.add(keyGen.next(), mo.Some(valGen.next()))
		nKeys += 1
	}
	sst, _ := writer.close()
	return sst, nKeys
}

func TestBuilderShouldMakeBlocksAvailable(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(32, 0, CompressionNone)
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()
	builder.add([]byte("aaaaaaaa"), mo.Some([]byte("11111111")))
	builder.add([]byte("bbbbbbbb"), mo.Some([]byte("22222222")))
	builder.add([]byte("cccccccc"), mo.Some([]byte("33333333")))

	iter := nextBlockToIter(builder)
	assertIterNextEntry(t, iter, []byte("aaaaaaaa"), []byte("11111111"))
	nextEntry, err := iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	iter = nextBlockToIter(builder)
	assertIterNextEntry(t, iter, []byte("bbbbbbbb"), []byte("22222222"))
	nextEntry, err = iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	assert.True(t, builder.nextBlock().IsAbsent())
	builder.add([]byte("dddddddd"), mo.Some([]byte("44444444")))

	iter = nextBlockToIter(builder)
	assertIterNextEntry(t, iter, []byte("cccccccc"), []byte("33333333"))
	nextEntry, err = iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	assert.True(t, builder.nextBlock().IsAbsent())
}

func TestBuilderShouldReturnUnconsumedBlocks(t *testing.T) {
	kvList := []KeyValue{
		{[]byte("aaaaaaaa"), []byte("11111111")},
		{[]byte("bbbbbbbb"), []byte("22222222")},
		{[]byte("cccccccc"), []byte("33333333")},
	}

	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(32, 0, CompressionNone)
	tableStore := newTableStore(bucket, format, "")
	builder := tableStore.tableBuilder()
	for _, kv := range kvList {
		builder.add(kv.key, mo.Some(kv.value))
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

	for i, kv := range kvList {
		block, err := format.readBlockRaw(encodedSST.sstInfo, uint64(i), rawSST)
		assert.NoError(t, err)
		iter := newBlockIteratorFromFirstKey(block)
		assertIterNextEntry(t, iter, kv.key, kv.value)
		nextEntry, err := iter.NextEntry()
		assert.NoError(t, err)
		assert.True(t, nextEntry.IsAbsent())
	}
}

func TestSSTable(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(4096, 0, CompressionNone)
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
	sstInfo := sstHandle.info.borrow()
	assert.Equal(t, 1, sstInfo.BlockMetaLength())
	assert.Equal(t, []byte("key1"), sstInfo.FirstKeyBytes())
	assert.Equal(t, []byte("key1"), sstInfo.UnPack().BlockMeta[0].FirstKey)

	// construct sst info from the raw bytes and validate that it matches the original info.
	sstHandleFromStore, err := tableStore.openSST(newSSTableIDWal(0))
	assert.NoError(t, err)
	assert.Equal(t, encodedInfo, sstHandleFromStore.info)

	sstInfoFromStore := sstHandleFromStore.info.borrow()
	assert.Equal(t, 1, sstInfoFromStore.BlockMetaLength())
	assert.Equal(t, []byte("key1"), sstInfoFromStore.FirstKeyBytes())
	assert.Equal(t, []byte("key1"), sstInfoFromStore.UnPack().BlockMeta[0].FirstKey)
}

func TestSSTableNoFilter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(4096, 3, CompressionNone)
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
	assert.Equal(t, uint64(0), sstHandle.info.borrow().FilterLen())
}

func TestSSTableWithCompression(t *testing.T) {
	codecs := []CompressionCodec{CompressionSnappy, CompressionZlib}
	for _, compression := range codecs {
		bucket := objstore.NewInMemBucket()
		format := newSSTableFormat(4096, 0, compression)
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
		sstInfo := sstHandle.info.borrow()
		assert.Equal(t, 1, sstInfo.BlockMetaLength())
		assert.Equal(t, []byte("key1"), sstInfo.FirstKeyBytes())
	}
}

func TestReadBlocks(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(32, 1, CompressionNone)
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
	blocks, err := format.readBlocks(encodedInfo, Range{0, 2}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(blocks))

	iter := newBlockIteratorFromFirstKey(&blocks[0])
	assertIterNextEntry(t, iter, []byte("aa"), []byte("11"))
	assertIterNextEntry(t, iter, []byte("bb"), []byte("22"))
	nextEntry, err := iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	iter = newBlockIteratorFromFirstKey(&blocks[1])
	assertIterNextEntry(t, iter, []byte("cccccccccccccccccccc"), []byte("33333333333333333333"))
	nextEntry, err = iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())
}

func TestReadAllBlocks(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(32, 1, CompressionNone)
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
	blocks, err := format.readBlocks(encodedInfo, Range{0, 3}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(blocks))

	iter := newBlockIteratorFromFirstKey(&blocks[0])
	assertIterNextEntry(t, iter, []byte("aa"), []byte("11"))
	assertIterNextEntry(t, iter, []byte("bb"), []byte("22"))
	nextEntry, err := iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	iter = newBlockIteratorFromFirstKey(&blocks[1])
	assertIterNextEntry(t, iter, []byte("cccccccccccccccccccc"), []byte("33333333333333333333"))
	nextEntry, err = iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())

	iter = newBlockIteratorFromFirstKey(&blocks[2])
	assertIterNextEntry(t, iter, []byte("dddddddddddddddddddd"), []byte("44444444444444444444"))
	nextEntry, err = iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsAbsent())
}

// SSTIterator tests

func TestOneBlockSSTIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(4096, 3, CompressionNone)
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
	assert.Equal(t, 1, sstHandle.info.borrow().BlockMetaLength())

	iter := newSSTIterator(sstHandle, tableStore, 1, 1)
	assertIterNext(t, iter, []byte("key1"), []byte("value1"))
	assertIterNext(t, iter, []byte("key2"), []byte("value2"))
	assertIterNext(t, iter, []byte("key3"), []byte("value3"))
	assertIterNext(t, iter, []byte("key4"), []byte("value4"))

	next, err := iter.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestManyBlockSSTIter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(4096, 3, CompressionNone)
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
	assert.Equal(t, 6, sstHandle.info.borrow().BlockMetaLength())

	iter := newSSTIterator(sstHandle, tableStore, 1, 1)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		assertIterNext(t, iter, key, value)
	}

	next, err := iter.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestIterFromKey(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(128, 1, CompressionNone)
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("aaaaaaaaaaaaaaaa")
	keyGen := newOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('z'))
	testCaseKeyGen := keyGen.clone()

	firstVal := []byte("1111111111111111")
	valGen := newOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	testCaseValGen := valGen.clone()

	sst, nKeys := buildSSTWithNBlocks(3, tableStore, keyGen, valGen)

	for i := 0; i < nKeys; i++ {
		expectedKeyGen := testCaseKeyGen.clone()
		expectedValGen := testCaseValGen.clone()
		fromKey := testCaseKeyGen.next()
		testCaseValGen.next()
		kvIter := newSSTIteratorFromKey(sst, tableStore, fromKey, 1, 1)

		for j := 0; j < nKeys-i; j++ {
			assertIterNext(t, kvIter, expectedKeyGen.next(), expectedValGen.next())
		}
		next, err := kvIter.Next()
		assert.NoError(t, err)
		assert.False(t, next.IsPresent())
	}
}

func TestIterFromKeySmallerThanFirst(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(128, 1, CompressionNone)
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("bbbbbbbbbbbbbbbb")
	keyGen := newOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('y'))
	expectedKeyGen := keyGen.clone()

	firstVal := []byte("2222222222222222")
	valGen := newOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))
	expectedValGen := valGen.clone()

	sst, nKeys := buildSSTWithNBlocks(2, tableStore, keyGen, valGen)
	kvIter := newSSTIteratorFromKey(sst, tableStore, []byte("aaaaaaaaaaaaaaaa"), 1, 1)

	for i := 0; i < nKeys; i++ {
		assertIterNext(t, kvIter, expectedKeyGen.next(), expectedValGen.next())
	}
	next, err := kvIter.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestIterFromKeyLargerThanLast(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := newSSTableFormat(128, 1, CompressionNone)
	tableStore := newTableStore(bucket, format, "")

	firstKey := []byte("bbbbbbbbbbbbbbbb")
	keyGen := newOrderedBytesGeneratorWithByteRange(firstKey, byte('a'), byte('y'))

	firstVal := []byte("2222222222222222")
	valGen := newOrderedBytesGeneratorWithByteRange(firstVal, byte(1), byte(26))

	sst, _ := buildSSTWithNBlocks(2, tableStore, keyGen, valGen)
	kvIter := newSSTIteratorFromKey(sst, tableStore, []byte("zzzzzzzzzzzzzzzz"), 1, 1)

	next, err := kvIter.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

func TestShouldGenerateOrderedBytes(t *testing.T) {
	suffix := make([]byte, SizeOfUint32InBytes)
	binary.BigEndian.PutUint32(suffix, 3735928559)
	start := []byte{0, 0, 0}
	gen := newOrderedBytesGenerator(suffix, start, 0, 2)
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
		assert.Equal(t, b, gen.next())
	}
}

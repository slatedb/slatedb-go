package slatedb

import (
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

func assertIterNextEntry(t *testing.T, iter *BlockIterator, key []byte, value []byte) {
	nextEntry, err := iter.NextEntry()
	assert.NoError(t, err)
	entry, ok := nextEntry.Get()
	assert.True(t, ok, "expected iterator to return a value")
	assert.Equal(t, key, entry.key)
	assert.Equal(t, value, entry.valueDel.value)
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
	kvList := []kv{
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
	blockCount := len(encodedSST.unconsumedBlocks)
	assert.Equal(t, 2, blockCount)

	for i := 0; i < blockCount; i++ {
		rawSST = append(rawSST, encodedSST.unconsumedBlocks[0]...)
		encodedSST.unconsumedBlocks = encodedSST.unconsumedBlocks[1:]
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
	sstHandle, err := tableStore.writeSST(ssTableIdWal(0), encodedSST)
	assert.NoError(t, err)
	assert.Equal(t, encodedInfo, sstHandle.info)
	sstInfo := sstHandle.info.borrow()
	assert.Equal(t, 1, sstInfo.BlockMetaLength())
	assert.Equal(t, []byte("key1"), sstInfo.FirstKeyBytes())
	assert.Equal(t, []byte("key1"), sstInfo.UnPack().BlockMeta[0].FirstKey)

	// construct sst info from the raw bytes and validate that it matches the original info.
	sstHandleFromStore, err := tableStore.openSST(ssTableIdWal(0))
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

	_, err = tableStore.writeSST(ssTableIdWal(0), encodedSST)
	assert.NoError(t, err)
	sstHandle, err := tableStore.openSST(ssTableIdWal(0))
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

		_, err = tableStore.writeSST(ssTableIdWal(0), encodedSST)
		assert.NoError(t, err)
		sstHandle, err := tableStore.openSST(ssTableIdWal(0))
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
	for _, b := range encodedSST.unconsumedBlocks {
		data = append(data, b...)
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
	for _, b := range encodedSST.unconsumedBlocks {
		data = append(data, b...)
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

func assertNext(t *testing.T, iter *SSTIterator, key []byte, value []byte) {
	next, err := iter.Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())
	kv, _ := next.Get()
	assert.Equal(t, key, kv.key)
	assert.Equal(t, value, kv.value)
}

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
	tableStore.writeSST(ssTableIdWal(0), encodedSST)
	sstHandle, err := tableStore.openSST(ssTableIdWal(0))
	assert.Equal(t, 1, sstHandle.info.borrow().BlockMetaLength())

	iter := newSSTIterator(sstHandle, tableStore, 1, 1)
	assertNext(t, iter, []byte("key1"), []byte("value1"))
	assertNext(t, iter, []byte("key2"), []byte("value2"))
	assertNext(t, iter, []byte("key3"), []byte("value3"))
	assertNext(t, iter, []byte("key4"), []byte("value4"))

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
	tableStore.writeSST(ssTableIdWal(0), encodedSST)
	sstHandle, err := tableStore.openSST(ssTableIdWal(0))
	assert.Equal(t, 6, sstHandle.info.borrow().BlockMetaLength())

	iter := newSSTIterator(sstHandle, tableStore, 1, 1)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		assertNext(t, iter, key, value)
	}

	next, err := iter.Next()
	assert.NoError(t, err)
	assert.False(t, next.IsPresent())
}

// TODO: add more tests

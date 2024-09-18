package slatedb

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	flatbuffers "github.com/google/flatbuffers/go"
	"hash/crc32"
	"io"
	"math"

	"github.com/golang/snappy"
	flatbuf "github.com/naveen246/slatedb-go/gen"
	"github.com/samber/mo"
)

// ------------------------------------------------
// SSTableFormat
// ------------------------------------------------

type SSTableFormat struct {
	blockSize        uint64
	minFilterKeys    uint32
	compressionCodec CompressionCodec
}

func newSSTableFormat(blockSize uint64, minFilterKeys uint32, compression CompressionCodec) *SSTableFormat {
	return &SSTableFormat{
		blockSize:        blockSize,
		minFilterKeys:    minFilterKeys,
		compressionCodec: compression,
	}
}

func (f *SSTableFormat) readInfo(obj ReadOnlyBlob) (*SSTableInfoOwned, error) {
	size, err := obj.len()
	if err != nil {
		return nil, err
	}
	if size <= 4 {
		return nil, ErrEmptySSTable
	}

	// Get the metadata. Last 4 bytes are the metadata offset of SsTableInfo
	offsetIndex := uint64(size - 4)
	offsetBytes, err := obj.readRange(Range{offsetIndex, uint64(size)})
	if err != nil {
		return nil, err
	}

	metadataOffset := binary.BigEndian.Uint32(offsetBytes)
	metadataBytes, err := obj.readRange(Range{uint64(metadataOffset), offsetIndex})
	if err != nil {
		return nil, err
	}

	return decodeBytesToSSTableInfo(metadataBytes)
}

func (f *SSTableFormat) readFilter(info *SSTableInfoOwned, obj ReadOnlyBlob) (mo.Option[BloomFilter], error) {
	sstInfo := info.borrow()
	if sstInfo.FilterLen() < 1 {
		return mo.None[BloomFilter](), nil
	}

	filterOffsetRange := Range{sstInfo.FilterOffset(), sstInfo.FilterOffset() + sstInfo.FilterLen()}
	filterBytes, err := obj.readRange(filterOffsetRange)
	if err != nil {
		return mo.None[BloomFilter](), err
	}

	filter := decodeBytesToBloomFilter(filterBytes)
	return mo.Some(*filter), nil
}

// decompress the compressed data using the specified compression codec.
// TODO: implement more compression options
func (f *SSTableFormat) decompress(compressedData []byte, compression CompressionCodec) ([]byte, error) {
	switch compression {
	case CompressionNone:
		return compressedData, nil
	case CompressionSnappy:
		return snappy.Decode(nil, compressedData)
	case CompressionZlib:
		r, err := zlib.NewReader(bytes.NewReader(compressedData))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	}

	return nil, ErrInvalidCompressionCodec
}

// getBlockRange returns the (startOffset, endOffset) of the data in ssTable that contains the
// blocks within blockRange
func (f *SSTableFormat) getBlockRange(blockRange Range, sstInfo *flatbuf.SsTableInfo) Range {
	blockMetaList := sstInfo.UnPack().BlockMeta
	startOffset := blockMetaList[blockRange.start].Offset

	endOffset := sstInfo.FilterOffset()
	if blockRange.end < uint64(sstInfo.BlockMetaLength()) {
		endOffset = blockMetaList[blockRange.end].Offset
	}

	return Range{startOffset, endOffset}
}

// readBlocks reads the complete data required into a byte slice (dataBytes)
// and then breaks the data up into slice of Blocks (decodedBlocks) which is returned
func (f *SSTableFormat) readBlocks(
	info *SSTableInfoOwned,
	blockRange Range,
	obj ReadOnlyBlob,
) ([]Block, error) {
	sstInfo := info.borrow()
	assertTrue(blockRange.start <= blockRange.end, "block start index cannot be greater than end index")
	assertTrue(blockRange.end <= uint64(sstInfo.BlockMetaLength()), "block end index out of range")

	if blockRange.start == blockRange.end {
		return []Block{}, nil
	}

	r := f.getBlockRange(blockRange, sstInfo)
	dataBytes, err := obj.readRange(r)
	if err != nil {
		return nil, err
	}

	startOffset := r.start
	decodedBlocks := make([]Block, 0)
	blockMetaList := sstInfo.UnPack().BlockMeta

	for i := blockRange.start; i < blockRange.end; i++ {
		bytesStart := blockMetaList[i].Offset - startOffset
		var blockBytes []byte
		if i == uint64(sstInfo.BlockMetaLength())-1 {
			blockBytes = dataBytes[bytesStart:]
		} else {
			bytesEnd := blockMetaList[i+1].Offset - startOffset
			blockBytes = dataBytes[bytesStart:bytesEnd]
		}

		decodedBlock, err := f.decodeBytesToBlock(blockBytes)
		if err != nil {
			return nil, err
		}
		decodedBlocks = append(decodedBlocks, *decodedBlock)
	}
	return decodedBlocks, nil
}

func (f *SSTableFormat) decodeBytesToBlock(bytes []byte) (*Block, error) {
	// last 4 bytes hold the checksum
	checksumIndex := len(bytes) - SizeOfUint32InBytes
	blockBytes := bytes[:checksumIndex]
	checksum := binary.BigEndian.Uint32(bytes[checksumIndex:])
	if checksum != crc32.ChecksumIEEE(blockBytes) {
		return nil, ErrChecksumMismatch
	}

	decodedBlock := decodeBytesToBlock(blockBytes)
	decompressedBytes, err := f.decompress(decodedBlock.data, f.compressionCodec)
	if err != nil {
		return nil, err
	}

	return &Block{
		data:    decompressedBytes,
		offsets: decodedBlock.offsets,
	}, nil
}

func (f *SSTableFormat) readBlock(
	info *SSTableInfoOwned,
	blockIndex uint64,
	obj ReadOnlyBlob,
) (*Block, error) {
	blocks, err := f.readBlocks(info, Range{blockIndex, blockIndex + 1}, obj)
	if err != nil {
		return nil, err
	}
	block := blocks[0]
	return &block, nil
}

func (f *SSTableFormat) readBlockRaw(
	info *SSTableInfoOwned,
	blockIndex uint64,
	sstBytes []byte,
) (*Block, error) {
	sstInfo := info.borrow()
	blockRange := f.getBlockRange(Range{blockIndex, blockIndex + 1}, sstInfo)
	return f.decodeBytesToBlock(sstBytes[blockRange.start:blockRange.end])
}

func (f *SSTableFormat) tableBuilder() *EncodedSSTableBuilder {
	return newEncodedSSTableBuilder(f.blockSize, f.minFilterKeys, f.compressionCodec)
}

// ------------------------------------------------
// EncodedSSTable
// ------------------------------------------------

type EncodedSSTable struct {
	sstInfo          *SSTableInfoOwned
	filter           mo.Option[BloomFilter]
	unconsumedBlocks [][]byte
}

type EncodedSSTableBuilder struct {
	blockBuilder   *BlockBuilder
	filterBuilder  *BloomFilterBuilder
	sstInfoBuilder *flatbuffers.Builder

	firstKey      mo.Option[[]byte]
	sstFirstKey   mo.Option[[]byte]
	blockMetaList []*flatbuf.BlockMetaT

	blocks        [][]byte
	blockSize     uint64
	currentLen    uint64
	minFilterKeys uint32
	numKeys       uint32

	compressionCodec CompressionCodec
}

// Create a builder based on target block size.
func newEncodedSSTableBuilder(
	blockSize uint64,
	minFilterKeys uint32,
	compressionCodec CompressionCodec,
) *EncodedSSTableBuilder {
	return &EncodedSSTableBuilder{
		blockBuilder:   newBlockBuilder(blockSize),
		filterBuilder:  newBloomFilterBuilder(10),
		sstInfoBuilder: flatbuffers.NewBuilder(0),

		firstKey:      mo.None[[]byte](),
		sstFirstKey:   mo.None[[]byte](),
		blockMetaList: []*flatbuf.BlockMetaT{},

		blocks:        [][]byte{},
		blockSize:     blockSize,
		currentLen:    0,
		minFilterKeys: minFilterKeys,
		numKeys:       0,

		compressionCodec: compressionCodec,
	}
}

func (b *EncodedSSTableBuilder) compress(data []byte, compression CompressionCodec) ([]byte, error) {
	switch compression {
	case CompressionNone:
		return data, nil
	case CompressionSnappy:
		return snappy.Encode(nil, data), nil
	case CompressionZlib:
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		defer w.Close()
		_, err := w.Write(data)
		if err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	}

	return nil, ErrInvalidCompressionCodec
}

func (b *EncodedSSTableBuilder) add(key []byte, value mo.Option[[]byte]) error {
	b.numKeys += 1

	if !b.blockBuilder.add(key, value) {
		// Create a new block builder and append block data
		blockBytes, err := b.finishBlock()
		if err != nil {
			return err
		}
		block, ok := blockBytes.Get()
		if ok {
			b.currentLen += uint64(len(block))
			b.blocks = append(b.blocks, block)
		}

		addSuccess := b.blockBuilder.add(key, value)
		assertTrue(addSuccess, "BlockBuilder add failed")
		b.firstKey = mo.Some(key)
	} else if b.sstFirstKey.IsAbsent() {
		b.sstFirstKey = mo.Some(key)
		b.firstKey = mo.Some(key)
	}

	b.filterBuilder.addKey(key)
	return nil
}

func (b *EncodedSSTableBuilder) nextBlock() mo.Option[[]byte] {
	if len(b.blocks) == 0 {
		return mo.None[[]byte]()
	}
	block := b.blocks[0]
	b.blocks = b.blocks[1:]
	return mo.Some(block)
}

func (b *EncodedSSTableBuilder) estimatedSize() uint64 {
	return b.currentLen
}

func (b *EncodedSSTableBuilder) finishBlock() (mo.Option[[]byte], error) {
	if b.blockBuilder.isEmpty() {
		return mo.None[[]byte](), nil
	}

	blockBuilder := b.blockBuilder
	b.blockBuilder = newBlockBuilder(b.blockSize)
	blk, err := blockBuilder.build()
	if err != nil {
		return mo.None[[]byte](), err
	}

	encodedBlock := blk.encodeToBytes()
	compressedBlock, err := b.compress(encodedBlock, b.compressionCodec)
	if err != nil {
		return mo.None[[]byte](), err
	}

	firstKey, _ := b.firstKey.Get()
	blockMetaT := flatbuf.BlockMetaT{Offset: b.currentLen, FirstKey: firstKey}
	b.blockMetaList = append(b.blockMetaList, &blockMetaT)

	checksum := crc32.ChecksumIEEE(compressedBlock)

	block := make([]byte, 0, len(compressedBlock)+SizeOfUint32InBytes)
	block = append(block, compressedBlock...)
	block = binary.BigEndian.AppendUint32(block, checksum)

	return mo.Some(block), nil
}

func (b *EncodedSSTableBuilder) build() (*EncodedSSTable, error) {
	blkBytes, err := b.finishBlock()
	if err != nil {
		return nil, err
	}
	buf, ok := blkBytes.Get()
	if !ok {
		buf = []byte{}
	}

	maybeFilter := mo.None[BloomFilter]()
	filterLen := 0
	filterOffset := b.currentLen + uint64(len(buf))
	if b.numKeys >= b.minFilterKeys {
		filter := b.filterBuilder.build()
		encodedFilter := filter.encodeToBytes()
		filterLen = len(encodedFilter)
		buf = append(buf, encodedFilter...)
		maybeFilter = mo.Some(*filter)
	}

	metaOffset := b.currentLen + uint64(len(buf))
	sstFirstKey, _ := b.sstFirstKey.Get()
	ssTableInfoT := flatbuf.SsTableInfoT{
		FirstKey:     sstFirstKey,
		BlockMeta:    b.blockMetaList,
		FilterOffset: filterOffset,
		FilterLen:    uint64(filterLen),
	}
	infoOffset := ssTableInfoT.Pack(b.sstInfoBuilder)
	b.sstInfoBuilder.Finish(infoOffset)

	info := newSSTableInfoOwned(b.sstInfoBuilder.FinishedBytes())
	info.encode(&buf)

	// write the metadata offset at the end of the file.
	buf = binary.BigEndian.AppendUint32(buf, uint32(metaOffset))
	b.blocks = append(b.blocks, buf)

	return &EncodedSSTable{
		sstInfo:          info,
		filter:           maybeFilter,
		unconsumedBlocks: b.blocks,
	}, nil
}

// ------------------------------------------------
// SSTableInfoOwned
// ------------------------------------------------

type SSTableInfoOwned struct {
	data []byte
}

func newSSTableInfoOwned(data []byte) *SSTableInfoOwned {
	return &SSTableInfoOwned{data: data}
}

func (info *SSTableInfoOwned) borrow() *flatbuf.SsTableInfo {
	return flatbuf.GetRootAsSsTableInfo(info.data, 0)
}

func (info *SSTableInfoOwned) encode(buf *[]byte) {
	*buf = append(*buf, info.data...)
	*buf = binary.BigEndian.AppendUint32(*buf, crc32.ChecksumIEEE(info.data))
}

func decodeBytesToSSTableInfo(rawBlockMeta []byte) (*SSTableInfoOwned, error) {
	if len(rawBlockMeta) <= SizeOfUint32InBytes {
		return nil, ErrEmptyBlockMeta
	}

	// last 4 bytes hold the checksum
	checksumIndex := len(rawBlockMeta) - SizeOfUint32InBytes
	data := rawBlockMeta[:checksumIndex]
	checksum := binary.BigEndian.Uint32(rawBlockMeta[checksumIndex:])
	if checksum != crc32.ChecksumIEEE(data) {
		return nil, ErrChecksumMismatch
	}

	return newSSTableInfoOwned(data), nil
}

// ------------------------------------------------
// SSTIterator
// ------------------------------------------------

type SSTIterator struct {
	table               *SSTableHandle
	tableStore          *TableStore
	currentBlockIter    mo.Option[*BlockIterator]
	fromKey             mo.Option[[]byte]
	nextBlockIdxToFetch uint64
	fetchTasks          chan chan mo.Option[[]Block]
	maxFetchTasks       uint64
	numBlocksToFetch    uint64
}

func newSSTIterator(
	table *SSTableHandle,
	tableStore *TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
) *SSTIterator {
	return &SSTIterator{
		table:               table,
		tableStore:          tableStore,
		currentBlockIter:    mo.None[*BlockIterator](),
		fromKey:             mo.None[[]byte](),
		nextBlockIdxToFetch: 0,
		fetchTasks:          make(chan chan mo.Option[[]Block], maxFetchTasks),
		maxFetchTasks:       maxFetchTasks,
		numBlocksToFetch:    numBlocksToFetch,
	}
}

func newSSTIteratorFromKey(
	table *SSTableHandle,
	tableStore *TableStore,
	fromKey []byte,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
) *SSTIterator {
	iter := &SSTIterator{
		table:            table,
		tableStore:       tableStore,
		currentBlockIter: mo.None[*BlockIterator](),
		fromKey:          mo.Some(fromKey),
		fetchTasks:       make(chan chan mo.Option[[]Block], maxFetchTasks),
		maxFetchTasks:    maxFetchTasks,
		numBlocksToFetch: numBlocksToFetch,
	}
	iter.nextBlockIdxToFetch = iter.firstBlockWithDataIncludingOrAfterKey(table, fromKey)
	return iter
}

func (iter *SSTIterator) Next() (mo.Option[KeyValue], error) {
	for {
		kvDel, err := iter.NextEntry()
		if err != nil {
			return mo.None[KeyValue](), err
		}
		keyVal, ok := kvDel.Get()
		if ok {
			if keyVal.valueDel.isTombstone {
				continue
			}

			return mo.Some[KeyValue](KeyValue{
				key:   keyVal.key,
				value: keyVal.valueDel.value,
			}), nil
		} else {
			return mo.None[KeyValue](), nil
		}
	}
}

func (iter *SSTIterator) NextEntry() (mo.Option[KeyValueDeletable], error) {
	for {
		if iter.currentBlockIter.IsAbsent() {
			nextBlockIter, err := iter.nextBlockIter()
			if err != nil {
				return mo.None[KeyValueDeletable](), err
			}

			if nextBlockIter.IsPresent() {
				iter.currentBlockIter = nextBlockIter
			} else {
				return mo.None[KeyValueDeletable](), nil
			}
		}

		currentBlockIter, _ := iter.currentBlockIter.Get()
		kv, err := currentBlockIter.NextEntry()
		if err != nil {
			return mo.None[KeyValueDeletable](), err
		}

		if kv.IsPresent() {
			return kv, nil
		} else {
			// We have exhausted the current block, but not necessarily the entire SST,
			// so we fall back to the top to check if we have more blocks to read.
			iter.currentBlockIter = mo.None[*BlockIterator]()
		}
	}
}

func (iter *SSTIterator) spawnFetches() {
	numBlocks := iter.table.info.borrow().BlockMetaLength()
	for len(iter.fetchTasks) < int(iter.maxFetchTasks) && int(iter.nextBlockIdxToFetch) < numBlocks {
		numBlocksToFetch := math.Min(
			float64(iter.numBlocksToFetch),
			float64(numBlocks-int(iter.nextBlockIdxToFetch)),
		)
		blocksStart := iter.nextBlockIdxToFetch
		blocksEnd := iter.nextBlockIdxToFetch + uint64(numBlocksToFetch)

		blocksCh := make(chan mo.Option[[]Block], 1)
		iter.fetchTasks <- blocksCh

		go func(table SSTableHandle, store TableStore, blocksStart uint64, blocksEnd uint64) {
			blocks, err := store.readBlocks(&table, Range{blocksStart, blocksEnd})
			if err != nil {
				blocksCh <- mo.None[[]Block]()
			} else {
				blocksCh <- mo.Some(blocks)
			}
		}(*iter.table, *iter.tableStore, blocksStart, blocksEnd)

		iter.nextBlockIdxToFetch = blocksEnd
	}
}

func (iter *SSTIterator) nextBlockIter() (mo.Option[*BlockIterator], error) {
	for {
		iter.spawnFetches()
		if len(iter.fetchTasks) == 0 {
			assertTrue(int(iter.nextBlockIdxToFetch) == iter.table.info.borrow().BlockMetaLength(), "")
			return mo.None[*BlockIterator](), nil
		}

		blocksCh := <-iter.fetchTasks
		blocks := <-blocksCh
		if blocks.IsPresent() {
			blks, _ := blocks.Get()
			if len(blks) == 0 {
				continue
			}

			block := &blks[0]
			fromKey, _ := iter.fromKey.Get()
			if iter.fromKey.IsPresent() {
				return mo.Some(newBlockIteratorFromKey(block, fromKey)), nil
			} else {
				return mo.Some(newBlockIteratorFromFirstKey(block)), nil
			}
		} else {
			return mo.None[*BlockIterator](), ErrReadBlocks
		}
	}
}

func (iter *SSTIterator) firstBlockWithDataIncludingOrAfterKey(sst *SSTableHandle, key []byte) uint64 {
	handle := sst.info.borrow()
	low := 0
	high := handle.BlockMetaLength() - 1
	// if the key is less than all the blocks' first key, scan the whole sst
	foundBlockID := 0

loop:
	for low <= high {
		mid := low + (high-low)/2
		midBlockFirstKey := handle.UnPack().BlockMeta[mid].FirstKey
		cmp := bytes.Compare(midBlockFirstKey, key)
		switch cmp {
		case -1:
			low = mid + 1
			foundBlockID = mid
		case 1:
			if mid > 0 {
				high = mid - 1
			} else {
				break loop
			}
		case 0:
			return uint64(mid)
		}
	}

	return uint64(foundBlockID)
}

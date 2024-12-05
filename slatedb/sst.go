package slatedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/gammazero/deque"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/sstable/bloom"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"go.uber.org/zap"
	"hash/crc32"
	"math"
	"sync"

	"github.com/samber/mo"
	flatbuf "github.com/slatedb/slatedb-go/gen"
)

// ------------------------------------------------
// SSTableFormat
// ------------------------------------------------

// SSTableFormat provides helper methods to read byte slice and get SSTable information
// SSTable holds list of blocks(each block is a list of KV pairs) and BloomFilter
// the helper methods mostly help in reading the blocks and the BloomFilter and also to decompress the data
// using the given compress.Codec
//
// TODO(thrawn01): Separate SSTableFormat into Decoder and Config structs
type SSTableFormat struct {
	// size of each block in the SSTable
	blockSize uint64

	// Write SSTables with a bloom filter if the number of keys in the SSTable is greater than or equal to this value.
	// Reads on small SSTables might be faster without a bloom filter.
	minFilterKeys uint32

	filterBitsPerKey uint32

	// defines how SSTable is encoded to byte slice and byte slice decoded to SSTable
	sstCodec sstable.SsTableInfoCodec

	// the codec used to compress/decompress SSTable before writing/reading from object storage
	compressionCodec compress.Codec
}

func defaultSSTableFormat() *SSTableFormat {
	return &SSTableFormat{
		blockSize:        4096,
		minFilterKeys:    0,
		filterBitsPerKey: 10,
		sstCodec:         FlatBufferSSTableInfoCodec{},
		compressionCodec: compress.CodecNone,
	}
}

func (f *SSTableFormat) readInfo(obj common.ReadOnlyBlob) (*sstable.SSTableInfo, error) {
	size, err := obj.Len()
	if err != nil {
		return nil, err
	}
	if size <= 4 {
		return nil, common.ErrEmptySSTable
	}

	// Get the metadata. Last 4 bytes are the metadata offset of SsTableInfo
	offsetIndex := uint64(size - 4)
	offsetBytes, err := obj.ReadRange(common.Range{Start: offsetIndex, End: uint64(size)})
	if err != nil {
		return nil, err
	}

	metadataOffset := binary.BigEndian.Uint32(offsetBytes)
	metadataBytes, err := obj.ReadRange(common.Range{Start: uint64(metadataOffset), End: offsetIndex})
	if err != nil {
		return nil, err
	}

	return decodeBytesToSSTableInfo(metadataBytes, f.sstCodec)
}

func (f *SSTableFormat) readFilter(sstInfo *sstable.SSTableInfo, obj common.ReadOnlyBlob) (mo.Option[bloom.Filter], error) {
	if sstInfo.FilterLen < 1 {
		return mo.None[bloom.Filter](), nil
	}

	filterOffsetRange := common.Range{
		Start: sstInfo.FilterOffset,
		End:   sstInfo.FilterOffset + sstInfo.FilterLen,
	}

	filterBytes, err := obj.ReadRange(filterOffsetRange)
	if err != nil {
		logger.Error("unable to read filter", zap.Error(err))
		return mo.None[bloom.Filter](), err
	}

	filterData, err := compress.Decode(filterBytes, sstInfo.CompressionCodec)
	if err != nil {
		return mo.Option[bloom.Filter]{}, err
	}
	return mo.Some(bloom.Decode(filterData)), nil
}

func (f *SSTableFormat) readIndex(info *sstable.SSTableInfo, obj common.ReadOnlyBlob) (*SSTableIndexData, error) {
	indexBytes, err := obj.ReadRange(common.Range{
		Start: info.IndexOffset,
		End:   info.IndexOffset + info.IndexLen,
	})
	if err != nil {
		return nil, err
	}

	data, err := compress.Decode(indexBytes, info.CompressionCodec)
	if err != nil {
		return nil, err
	}

	return newSSTableIndexData(data), nil
}

func (f *SSTableFormat) readIndexRaw(info *sstable.SSTableInfo, sstBytes []byte) (*SSTableIndexData, error) {
	indexBytes := sstBytes[info.IndexOffset : info.IndexOffset+info.IndexLen]

	data, err := compress.Decode(indexBytes, info.CompressionCodec)
	if err != nil {
		return nil, err
	}

	return newSSTableIndexData(data), nil
}

// getBlockRange returns the (startOffset, endOffset) of the data in ssTable that contains the
// blocks within rng
func (f *SSTableFormat) getBlockRange(rng common.Range, sstInfo *sstable.SSTableInfo, index *flatbuf.SsTableIndex) common.Range {
	blockMetaList := index.UnPack().BlockMeta
	startOffset := blockMetaList[rng.Start].Offset

	endOffset := sstInfo.FilterOffset
	if rng.End < uint64(len(blockMetaList)) {
		endOffset = blockMetaList[rng.End].Offset
	}

	return common.Range{Start: startOffset, End: endOffset}
}

// readBlocks reads the complete data required into a byte slice (dataBytes)
// and then breaks the data up into slice of Blocks (decodedBlocks) which is returned
func (f *SSTableFormat) readBlocks(
	sstInfo *sstable.SSTableInfo,
	indexData *SSTableIndexData,
	blockRange common.Range,
	obj common.ReadOnlyBlob,
) ([]block.Block, error) {
	index := indexData.ssTableIndex()
	common.AssertTrue(blockRange.Start <= blockRange.End, "block start index cannot be greater than end index")
	common.AssertTrue(blockRange.End <= uint64(index.BlockMetaLength()), "block end index out of range")

	if blockRange.Start == blockRange.End {
		return []block.Block{}, nil
	}

	rng := f.getBlockRange(blockRange, sstInfo, index)
	dataBytes, err := obj.ReadRange(rng)
	if err != nil {
		logger.Error("unable to read block data", zap.Error(err))
		return nil, err
	}

	startOffset := rng.Start
	decodedBlocks := make([]block.Block, 0)
	blockMetaList := index.UnPack().BlockMeta
	compressionCodec := sstInfo.CompressionCodec

	for i := blockRange.Start; i < blockRange.End; i++ {
		bytesStart := blockMetaList[i].Offset - startOffset
		var blockBytes []byte
		if i == uint64(index.BlockMetaLength())-1 {
			blockBytes = dataBytes[bytesStart:]
		} else {
			bytesEnd := blockMetaList[i+1].Offset - startOffset
			blockBytes = dataBytes[bytesStart:bytesEnd]
		}

		decodedBlock, err := f.decodeBytesToBlock(blockBytes, compressionCodec)
		if err != nil {
			logger.Error("unable to decode block", zap.Error(err))
			return nil, err
		}
		decodedBlocks = append(decodedBlocks, *decodedBlock)
	}
	return decodedBlocks, nil
}

func (f *SSTableFormat) decodeBytesToBlock(bytes []byte, compressionCodec compress.Codec) (*block.Block, error) {
	// last 4 bytes hold the checksum
	checksumIndex := len(bytes) - common.SizeOfUint32
	blockBytes := bytes[:checksumIndex]
	storedChecksum := binary.BigEndian.Uint32(bytes[checksumIndex:])
	if storedChecksum != crc32.ChecksumIEEE(blockBytes) {
		logger.Error("checksum does not match")
		return nil, common.ErrChecksumMismatch
	}

	var decodedBlock block.Block
	if err := block.Decode(&decodedBlock, blockBytes); err != nil {
		return nil, err
	}

	decompressedBytes, err := compress.Decode(decodedBlock.Data, compressionCodec)
	if err != nil {
		return nil, err
	}

	return &block.Block{
		Data:    decompressedBytes,
		Offsets: decodedBlock.Offsets,
	}, nil
}

func (f *SSTableFormat) readBlock(
	info *sstable.SSTableInfo,
	indexData *SSTableIndexData,
	blockIndex uint64,
	obj common.ReadOnlyBlob,
) (*block.Block, error) {
	blocks, err := f.readBlocks(info, indexData, common.Range{Start: blockIndex, End: blockIndex + 1}, obj)
	if err != nil {
		return nil, err
	}
	block := blocks[0]
	return &block, nil
}

func (f *SSTableFormat) readBlockRaw(
	info *sstable.SSTableInfo,
	indexData *SSTableIndexData,
	blockIndex uint64,
	sstBytes []byte,
) (*block.Block, error) {
	index := indexData.ssTableIndex()
	blockRange := f.getBlockRange(common.Range{Start: blockIndex, End: blockIndex + 1}, info, index)
	return f.decodeBytesToBlock(sstBytes[blockRange.Start:blockRange.End], info.CompressionCodec)
}

func (f *SSTableFormat) tableBuilder() *EncodedSSTableBuilder {
	return newEncodedSSTableBuilder(
		f.blockSize,
		f.minFilterKeys,
		f.sstCodec,
		f.filterBitsPerKey,
		f.compressionCodec,
	)
}

func (f *SSTableFormat) clone() *SSTableFormat {
	return &SSTableFormat{
		blockSize:        f.blockSize,
		minFilterKeys:    f.minFilterKeys,
		compressionCodec: f.compressionCodec,
	}
}

// EncodedSSTable
// TODO(thrawn01): Rename this to sstable.Encoded or sstable.Table
type EncodedSSTable struct {
	sstInfo *sstable.SSTableInfo
	filter  mo.Option[bloom.Filter]

	// unconsumedBlocks contains a list of blocks that have not yet been written to object storage.
	// It is initialized to the complete list of blocks in the SSTable
	unconsumedBlocks *deque.Deque[[]byte]
}

// EncodedSSTableBuilder - we use an EncodedSSTableBuilder to build an EncodedSSTable before writing
// the EncodedSSTable to object storage.
// The builder provides helper methods to encode the SSTable to byte slice using flatbuffers and also to compress
// the data using the given compress.Codec
// TODO(thrawn01): Rename this to sstable.Builder
type EncodedSSTableBuilder struct {
	blockBuilder  *block.Builder
	filterBuilder *bloom.Builder

	// The metadata for each block held by the SSTableIndex
	blockMetaList []*flatbuf.BlockMetaT

	// holds the firstKey of the current block that is being built.
	// when the current block reaches BlockSize, a new Block is created and firstKey will then hold the first key
	// of the new block
	firstKey mo.Option[[]byte]

	// holds the first key of the SSTable
	sstFirstKey mo.Option[[]byte]

	// The encoded/serialized blocks that get added to the SSTable
	blocks *deque.Deque[[]byte]

	blockSize uint64

	// currentLen is the total length of all existing blocks
	currentLen uint64

	// if numKeys >= minFilterKeys then we add a BloomFilter
	// else we don't add BloomFilter since reading smaller set of keys is likely faster without BloomFilter
	minFilterKeys uint32
	numKeys       uint32

	sstCodec         sstable.SsTableInfoCodec
	compressionCodec compress.Codec
}

// Create a builder based on target block size.
func newEncodedSSTableBuilder(
	blockSize uint64,
	minFilterKeys uint32,
	sstCodec sstable.SsTableInfoCodec,
	filterBitsPerKey uint32,
	compressionCodec compress.Codec,
) *EncodedSSTableBuilder {
	return &EncodedSSTableBuilder{
		blockBuilder:  block.NewBuilder(blockSize),
		filterBuilder: bloom.NewBuilder(filterBitsPerKey),

		firstKey:      mo.None[[]byte](),
		sstFirstKey:   mo.None[[]byte](),
		blockMetaList: []*flatbuf.BlockMetaT{},

		blocks:        deque.New[[]byte](0),
		blockSize:     blockSize,
		currentLen:    0,
		minFilterKeys: minFilterKeys,
		numKeys:       0,

		sstCodec:         sstCodec,
		compressionCodec: compressionCodec,
	}
}

func (b *EncodedSSTableBuilder) add(key []byte, value mo.Option[[]byte]) error {
	b.numKeys += 1

	if !b.blockBuilder.Add(key, value) {
		// Create a new block builder and append block data
		blockBytes, err := b.finishBlock()
		if err != nil {
			return err
		}
		block, ok := blockBytes.Get()
		if ok {
			b.currentLen += uint64(len(block))
			b.blocks.PushBack(block)
		}

		addSuccess := b.blockBuilder.Add(key, value)
		common.AssertTrue(addSuccess, "block.Builder add failed")
		b.firstKey = mo.Some(key)
	} else if b.sstFirstKey.IsAbsent() {
		b.sstFirstKey = mo.Some(key)
		b.firstKey = mo.Some(key)
	}

	b.filterBuilder.Add(key)
	return nil
}

func (b *EncodedSSTableBuilder) nextBlock() mo.Option[[]byte] {
	if b.blocks.Len() == 0 {
		return mo.None[[]byte]()
	}
	block := b.blocks.PopFront()
	return mo.Some(block)
}

func (b *EncodedSSTableBuilder) estimatedSize() uint64 {
	return b.currentLen
}

func (b *EncodedSSTableBuilder) finishBlock() (mo.Option[[]byte], error) {
	if b.blockBuilder.IsEmpty() {
		return mo.None[[]byte](), nil
	}

	blockBuilder := b.blockBuilder
	b.blockBuilder = block.NewBuilder(b.blockSize)
	blk, err := blockBuilder.Build()
	if err != nil {
		return mo.None[[]byte](), err
	}

	encodedBlock := block.Encode(blk)
	compressedBlock, err := compress.Encode(encodedBlock, b.compressionCodec)
	if err != nil {
		return mo.None[[]byte](), err
	}

	firstKey, _ := b.firstKey.Get()
	blockMeta := flatbuf.BlockMetaT{Offset: b.currentLen, FirstKey: firstKey}
	b.blockMetaList = append(b.blockMetaList, &blockMeta)

	checksum := crc32.ChecksumIEEE(compressedBlock)

	block := make([]byte, 0, len(compressedBlock)+common.SizeOfUint32)
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

	maybeFilter := mo.None[bloom.Filter]()
	filterLen := 0
	filterOffset := b.currentLen + uint64(len(buf))
	if b.numKeys >= b.minFilterKeys {
		filter := b.filterBuilder.Build()
		compressedFilter, err := compress.Encode(bloom.Encode(filter), b.compressionCodec)
		if err != nil {
			return nil, err
		}
		filterLen = len(compressedFilter)
		buf = append(buf, compressedFilter...)
		maybeFilter = mo.Some(filter)
	}

	// write the index block
	sstIndex := flatbuf.SsTableIndexT{BlockMeta: b.blockMetaList}
	indexBlock := FlatBufferSSTableIndexCodec{}.encode(sstIndex)
	compressedIndexBlock, err := compress.Encode(indexBlock, b.compressionCodec)
	if err != nil {
		return nil, err
	}
	indexOffset := b.currentLen + uint64(len(buf))
	buf = append(buf, compressedIndexBlock...)

	metaOffset := b.currentLen + uint64(len(buf))
	sstInfo := &sstable.SSTableInfo{
		FirstKey:         b.sstFirstKey,
		IndexOffset:      indexOffset,
		IndexLen:         uint64(len(compressedIndexBlock)),
		FilterOffset:     filterOffset,
		FilterLen:        uint64(filterLen),
		CompressionCodec: b.compressionCodec,
	}
	sstInfo.Encode(&buf, b.sstCodec)

	// write the metadata offset at the end of the file.
	buf = binary.BigEndian.AppendUint32(buf, uint32(metaOffset))
	b.blocks.PushBack(buf)

	return &EncodedSSTable{
		sstInfo:          sstInfo,
		filter:           maybeFilter,
		unconsumedBlocks: b.blocks,
	}, nil
}

// ------------------------------------------------
// SSTableInfo
// ------------------------------------------------

func decodeBytesToSSTableInfo(rawInfo []byte, sstCodec sstable.SsTableInfoCodec) (*sstable.SSTableInfo, error) {
	if len(rawInfo) <= common.SizeOfUint32 {
		return nil, common.ErrEmptyBlockMeta
	}

	// last 4 bytes hold the checksum
	checksumIndex := len(rawInfo) - common.SizeOfUint32
	data := rawInfo[:checksumIndex]
	checksum := binary.BigEndian.Uint32(rawInfo[checksumIndex:])
	if checksum != crc32.ChecksumIEEE(data) {
		logger.Error("check sum does not match")
		return nil, common.ErrChecksumMismatch
	}

	info := sstCodec.Decode(data)
	return info, nil
}

// ------------------------------------------------
// SSTIterator
// ------------------------------------------------

// SSTIterator helps in iterating through KeyValue pairs present in the SSTable.
// Since each SSTable is a list of Blocks, this iterator internally uses block.Iterator to iterate through each Block
type SSTIterator struct {
	table               *SSTableHandle
	indexData           *SSTableIndexData
	tableStore          *TableStore
	currentBlockIter    mo.Option[*block.Iterator]
	fromKey             mo.Option[[]byte]
	nextBlockIdxToFetch uint64
	fetchTasks          chan chan mo.Option[[]block.Block]
	maxFetchTasks       uint64
	numBlocksToFetch    uint64
}

func newSSTIterator(
	table *SSTableHandle,
	tableStore *TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
) (*SSTIterator, error) {
	indexData, err := tableStore.readIndex(table)
	if err != nil {
		return nil, err
	}

	return &SSTIterator{
		table:               table,
		indexData:           indexData,
		tableStore:          tableStore,
		currentBlockIter:    mo.None[*block.Iterator](),
		fromKey:             mo.None[[]byte](),
		nextBlockIdxToFetch: 0,
		fetchTasks:          make(chan chan mo.Option[[]block.Block], maxFetchTasks),
		maxFetchTasks:       maxFetchTasks,
		numBlocksToFetch:    numBlocksToFetch,
	}, nil
}

func newSSTIteratorFromKey(
	table *SSTableHandle,
	fromKey []byte,
	tableStore *TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
) (*SSTIterator, error) {
	indexData, err := tableStore.readIndex(table)
	if err != nil {
		return nil, err
	}

	iter := &SSTIterator{
		table:            table,
		indexData:        indexData,
		tableStore:       tableStore,
		currentBlockIter: mo.None[*block.Iterator](),
		fromKey:          mo.Some(fromKey),
		fetchTasks:       make(chan chan mo.Option[[]block.Block], maxFetchTasks),
		maxFetchTasks:    maxFetchTasks,
		numBlocksToFetch: numBlocksToFetch,
	}
	iter.nextBlockIdxToFetch = iter.firstBlockWithDataIncludingOrAfterKey(indexData, fromKey)
	return iter, nil
}

func (iter *SSTIterator) Next() (common.KV, bool) {
	for {
		keyVal, ok := iter.NextEntry()
		if !ok {
			return common.KV{}, false
		}

		if keyVal.ValueDel.IsTombstone {
			continue
		}

		return common.KV{
			Key:   keyVal.Key,
			Value: keyVal.ValueDel.Value,
		}, true
	}
}

func (iter *SSTIterator) NextEntry() (common.KVDeletable, bool) {
	for {
		if iter.currentBlockIter.IsAbsent() {
			nextBlockIter, err := iter.nextBlockIter()
			if err != nil {
				return common.KVDeletable{}, false
			}

			if nextBlockIter.IsPresent() {
				iter.currentBlockIter = nextBlockIter
			} else {
				return common.KVDeletable{}, false
			}
		}

		currentBlockIter, _ := iter.currentBlockIter.Get()
		kv, ok := currentBlockIter.NextEntry()
		if !ok {
			// We have exhausted the current block, but not necessarily the entire SST,
			// so we fall back to the top to check if we have more blocks to read.
			iter.currentBlockIter = mo.None[*block.Iterator]()
			continue
		}

		return kv, true
	}
}

// spawnFetches - Each SST has multiple blocks, this method will create goroutines to fetch blocks within a range
// Range{blocksStart, blocksEnd} for a given SST from object storage
func (iter *SSTIterator) spawnFetches() {

	numBlocks := iter.indexData.ssTableIndex().BlockMetaLength()
	table := iter.table.clone()
	tableStore := iter.tableStore.clone()
	index := iter.indexData.clone()
	var wg sync.WaitGroup

	for len(iter.fetchTasks) < int(iter.maxFetchTasks) && int(iter.nextBlockIdxToFetch) < numBlocks {
		numBlocksToFetch := math.Min(
			float64(iter.numBlocksToFetch),
			float64(numBlocks-int(iter.nextBlockIdxToFetch)),
		)
		blocksStart := iter.nextBlockIdxToFetch
		blocksEnd := iter.nextBlockIdxToFetch + uint64(numBlocksToFetch)

		blocksCh := make(chan mo.Option[[]block.Block], 1)
		iter.fetchTasks <- blocksCh

		blocksRange := common.Range{Start: blocksStart, End: blocksEnd}

		wg.Add(1)
		go func() {
			blocks, err := tableStore.readBlocksUsingIndex(table, blocksRange, index)
			if err != nil {
				blocksCh <- mo.None[[]block.Block]()
			} else {
				blocksCh <- mo.Some(blocks)
			}
			wg.Done()
		}()
		wg.Wait()

		iter.nextBlockIdxToFetch = blocksEnd
	}
}

func (iter *SSTIterator) nextBlockIter() (mo.Option[*block.Iterator], error) {
	for {
		iter.spawnFetches()
		// TODO(thrawn01): This is a race, we should not expect an empty channel to indicate there are no more
		//  items to process.
		if len(iter.fetchTasks) == 0 {
			common.AssertTrue(int(iter.nextBlockIdxToFetch) == iter.indexData.ssTableIndex().BlockMetaLength(), "")
			fmt.Printf("Iteration Stopped Due To Empty Task Channel\n")
			return mo.None[*block.Iterator](), nil
		}

		blocksCh := <-iter.fetchTasks
		blocks := <-blocksCh
		if blocks.IsPresent() {
			blks, _ := blocks.Get()
			if len(blks) == 0 {
				continue
			}

			b := &blks[0]
			fromKey, _ := iter.fromKey.Get()
			if iter.fromKey.IsPresent() {
				return mo.Some(block.NewIteratorAtKey(b, fromKey)), nil
			} else {
				return mo.Some(block.NewIterator(b)), nil
			}
		} else {
			logger.Error("unable to read block")
			return mo.None[*block.Iterator](), common.ErrReadBlocks
		}
	}
}

func (iter *SSTIterator) firstBlockWithDataIncludingOrAfterKey(indexData *SSTableIndexData, key []byte) uint64 {
	sstIndex := indexData.ssTableIndex()
	low := 0
	high := sstIndex.BlockMetaLength() - 1
	// if the key is less than all the blocks' first key, scan the whole sst
	foundBlockID := 0

loop:
	for low <= high {
		mid := low + (high-low)/2
		midBlockFirstKey := sstIndex.UnPack().BlockMeta[mid].FirstKey
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

package slatedb

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"hash/crc32"
	"io"
	"math"

	"github.com/gammazero/deque"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/filter"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"go.uber.org/zap"

	"github.com/golang/snappy"
	"github.com/samber/mo"
	flatbuf "github.com/slatedb/slatedb-go/gen"
)

// ------------------------------------------------
// SSTableFormat
// ------------------------------------------------

type SSTableFormat struct {
	blockSize        uint64
	minFilterKeys    uint32
	sstCodec         SsTableInfoCodec
	filterBitsPerKey uint32
	compressionCodec CompressionCodec
	rowFeatures      []RowFeature
}

func defaultSSTableFormat() *SSTableFormat {
	return &SSTableFormat{
		blockSize:        4096,
		minFilterKeys:    0,
		sstCodec:         &FlatBufferSSTableInfoCodec{},
		filterBitsPerKey: 10,
		compressionCodec: CompressionNone,
		rowFeatures:      []RowFeature{RowFeatureFlags, RowFeatureTimestamp},
	}
}

func (f *SSTableFormat) readInfo(obj common.ReadOnlyBlob) (*SSTableInfo, error) {
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

func (f *SSTableFormat) readFilter(sstInfo *SSTableInfo, obj common.ReadOnlyBlob) (mo.Option[filter.BloomFilter], error) {
	if sstInfo.filterLen < 1 {
		return mo.None[filter.BloomFilter](), nil
	}

	filterOffsetRange := common.Range{
		Start: sstInfo.filterOffset,
		End:   sstInfo.filterOffset + sstInfo.filterLen,
	}
	filterBytes, err := obj.ReadRange(filterOffsetRange)
	if err != nil {
		logger.Error("unable to read filter", zap.Error(err))
		return mo.None[filter.BloomFilter](), err
	}

	filterData, err := f.decompress(filterBytes, sstInfo.compressionCodec)
	if err != nil {
		return mo.Option[filter.BloomFilter]{}, err
	}
	filtr := filter.DecodeBytesToBloomFilter(filterData)
	return mo.Some(*filtr), nil
}

func (f *SSTableFormat) readIndex(info *SSTableInfo, obj common.ReadOnlyBlob) (*SSTableIndexData, error) {
	indexBytes, err := obj.ReadRange(common.Range{
		Start: info.indexOffset,
		End:   info.indexOffset + info.indexLen,
	})
	if err != nil {
		return nil, err
	}

	data, err := f.decompress(indexBytes, info.compressionCodec)
	if err != nil {
		return nil, err
	}

	return newSSTableIndexData(data), nil
}

func (f *SSTableFormat) readIndexRaw(info *SSTableInfo, sstBytes []byte) (*SSTableIndexData, error) {
	indexBytes := sstBytes[info.indexOffset : info.indexOffset+info.indexLen]

	data, err := f.decompress(indexBytes, info.compressionCodec)
	if err != nil {
		return nil, err
	}

	return newSSTableIndexData(data), nil
}

// decompress the compressed data using the specified compression codec.
func (f *SSTableFormat) decompress(compressedData []byte, compression CompressionCodec) ([]byte, error) {
	if compression == CompressionNone {
		return compressedData, nil
	}

	buf := bytes.NewBuffer(compressedData)

	switch compression {
	case CompressionSnappy:
		r := snappy.NewReader(buf)
		return io.ReadAll(r)
	case CompressionZlib:
		r, err := zlib.NewReader(buf)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case CompressionLz4:
		r := lz4.NewReader(buf)
		return io.ReadAll(r)
	case CompressionZstd:
		r, err := zstd.NewReader(buf)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	default:
		return nil, common.ErrInvalidCompressionCodec
	}
}

// getBlockRange returns the (startOffset, endOffset) of the data in ssTable that contains the
// blocks within rng
func (f *SSTableFormat) getBlockRange(rng common.Range, sstInfo *SSTableInfo, index *flatbuf.SsTableIndex) common.Range {
	blockMetaList := index.UnPack().BlockMeta
	startOffset := blockMetaList[rng.Start].Offset

	endOffset := sstInfo.filterOffset
	if rng.End < uint64(len(blockMetaList)) {
		endOffset = blockMetaList[rng.End].Offset
	}

	return common.Range{Start: startOffset, End: endOffset}
}

// readBlocks reads the complete data required into a byte slice (dataBytes)
// and then breaks the data up into slice of Blocks (decodedBlocks) which is returned
func (f *SSTableFormat) readBlocks(
	sstInfo *SSTableInfo,
	indexData *SSTableIndexData,
	blockRange common.Range,
	obj common.ReadOnlyBlob,
) ([]Block, error) {
	index := indexData.ssTableIndex()
	common.AssertTrue(blockRange.Start <= blockRange.End, "block start index cannot be greater than end index")
	common.AssertTrue(blockRange.End <= uint64(index.BlockMetaLength()), "block end index out of range")

	if blockRange.Start == blockRange.End {
		return []Block{}, nil
	}

	rng := f.getBlockRange(blockRange, sstInfo, index)
	dataBytes, err := obj.ReadRange(rng)
	if err != nil {
		logger.Error("unable to read block data", zap.Error(err))
		return nil, err
	}

	startOffset := rng.Start
	decodedBlocks := make([]Block, 0)
	blockMetaList := index.UnPack().BlockMeta
	compressionCodec := sstInfo.compressionCodec

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

func (f *SSTableFormat) decodeBytesToBlock(bytes []byte, compressionCodec CompressionCodec) (*Block, error) {
	// last 4 bytes hold the checksum
	checksumIndex := len(bytes) - common.SizeOfUint32InBytes
	blockBytes := bytes[:checksumIndex]
	storedChecksum := binary.BigEndian.Uint32(bytes[checksumIndex:])
	if storedChecksum != crc32.ChecksumIEEE(blockBytes) {
		logger.Error("checksum does not match")
		return nil, common.ErrChecksumMismatch
	}

	decodedBlock := decodeBytesToBlock(blockBytes)
	decompressedBytes, err := f.decompress(decodedBlock.data, compressionCodec)
	if err != nil {
		return nil, err
	}

	return &Block{
		data:    decompressedBytes,
		offsets: decodedBlock.offsets,
	}, nil
}

func (f *SSTableFormat) readBlock(
	info *SSTableInfo,
	indexData *SSTableIndexData,
	blockIndex uint64,
	obj common.ReadOnlyBlob,
) (*Block, error) {
	blocks, err := f.readBlocks(info, indexData, common.Range{Start: blockIndex, End: blockIndex + 1}, obj)
	if err != nil {
		return nil, err
	}
	block := blocks[0]
	return &block, nil
}

func (f *SSTableFormat) readBlockRaw(
	info *SSTableInfo,
	indexData *SSTableIndexData,
	blockIndex uint64,
	sstBytes []byte,
) (*Block, error) {
	index := indexData.ssTableIndex()
	blockRange := f.getBlockRange(common.Range{Start: blockIndex, End: blockIndex + 1}, info, index)
	return f.decodeBytesToBlock(sstBytes[blockRange.Start:blockRange.End], info.compressionCodec)
}

func (f *SSTableFormat) tableBuilder() *EncodedSSTableBuilder {
	return newEncodedSSTableBuilder(f.blockSize, f.minFilterKeys, f.compressionCodec)
}

func (f *SSTableFormat) clone() *SSTableFormat {
	return &SSTableFormat{
		blockSize:        f.blockSize,
		minFilterKeys:    f.minFilterKeys,
		compressionCodec: f.compressionCodec,
	}
}

// ------------------------------------------------
// EncodedSSTable
// ------------------------------------------------

type EncodedSSTable struct {
	sstInfo          *SSTableInfo
	filter           mo.Option[filter.BloomFilter]
	unconsumedBlocks *deque.Deque[[]byte]
}

type EncodedSSTableBuilder struct {
	blockBuilder  *BlockBuilder
	filterBuilder *filter.BloomFilterBuilder
	indexBuilder  *flatbuffers.Builder

	firstKey      mo.Option[[]byte]
	sstFirstKey   mo.Option[[]byte]
	blockMetaList []*flatbuf.BlockMetaT

	blocks        *deque.Deque[[]byte]
	rowFeatures   []RowFeature
	blockSize     uint64
	currentLen    uint64
	minFilterKeys uint32
	numKeys       uint32

	sstCodec         SsTableInfoCodec
	compressionCodec CompressionCodec
}

// Create a builder based on target block size.
func newEncodedSSTableBuilder(
	blockSize uint64,
	minFilterKeys uint32,
	sstCodec SsTableInfoCodec,
	filterBitsPerKey uint32,
	compressionCodec CompressionCodec,
	rowFeatures []RowFeature,
) *EncodedSSTableBuilder {
	return &EncodedSSTableBuilder{
		blockBuilder:  newBlockBuilder(blockSize, rowFeatures),
		filterBuilder: filter.NewBloomFilterBuilder(filterBitsPerKey),
		indexBuilder:  flatbuffers.NewBuilder(0),

		firstKey:      mo.None[[]byte](),
		sstFirstKey:   mo.None[[]byte](),
		blockMetaList: []*flatbuf.BlockMetaT{},

		blocks:        deque.New[[]byte](0),
		rowFeatures:   rowFeatures,
		blockSize:     blockSize,
		currentLen:    0,
		minFilterKeys: minFilterKeys,
		numKeys:       0,

		sstCodec:         sstCodec,
		compressionCodec: compressionCodec,
	}
}

func (b *EncodedSSTableBuilder) compress(data []byte, compression CompressionCodec) ([]byte, error) {
	if compression == CompressionNone {
		return data, nil
	}

	var buf bytes.Buffer
	var w io.WriteCloser
	var err error

	switch compression {
	case CompressionSnappy:
		w = snappy.NewBufferedWriter(&buf)
	case CompressionZlib:
		w = zlib.NewWriter(&buf)
	case CompressionLz4:
		w = lz4.NewWriter(&buf)
	case CompressionZstd:
		w, err = zstd.NewWriter(&buf)
		if err != nil {
			return nil, err
		}
	default:
		return nil, common.ErrInvalidCompressionCodec
	}

	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
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
			b.blocks.PushBack(block)
		}

		addSuccess := b.blockBuilder.add(key, value)
		common.AssertTrue(addSuccess, "BlockBuilder add failed")
		b.firstKey = mo.Some(key)
	} else if b.sstFirstKey.IsAbsent() {
		b.sstFirstKey = mo.Some(key)
		b.firstKey = mo.Some(key)
	}

	b.filterBuilder.AddKey(key)
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

	block := make([]byte, 0, len(compressedBlock)+common.SizeOfUint32InBytes)
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

	maybeFilter := mo.None[filter.BloomFilter]()
	filterLen := 0
	filterOffset := b.currentLen + uint64(len(buf))
	if b.numKeys >= b.minFilterKeys {
		filtr := b.filterBuilder.Build()
		encodedFilter := filtr.EncodeToBytes()
		filterLen = len(encodedFilter)
		buf = append(buf, encodedFilter...)
		maybeFilter = mo.Some(*filtr)
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

	info := newSSTableInfo(b.sstInfoBuilder.FinishedBytes())
	info.encode(&buf)

	// write the metadata offset at the end of the file.
	buf = binary.BigEndian.AppendUint32(buf, uint32(metaOffset))
	b.blocks.PushBack(buf)

	return &EncodedSSTable{
		sstInfo:          info,
		filter:           maybeFilter,
		unconsumedBlocks: b.blocks,
	}, nil
}

// ------------------------------------------------
// SSTableInfo
// ------------------------------------------------

func (info *SSTableInfo) encode(buf *[]byte, sstCodec SsTableInfoCodec) {
	data := sstCodec.encode(info)
	*buf = append(*buf, data...)
	*buf = binary.BigEndian.AppendUint32(*buf, crc32.ChecksumIEEE(data))
}

func decodeBytesToSSTableInfo(rawInfo []byte, sstCodec SsTableInfoCodec) (*SSTableInfo, error) {
	if len(rawInfo) <= common.SizeOfUint32InBytes {
		return nil, common.ErrEmptyBlockMeta
	}

	// last 4 bytes hold the checksum
	checksumIndex := len(rawInfo) - common.SizeOfUint32InBytes
	data := rawInfo[:checksumIndex]
	checksum := binary.BigEndian.Uint32(rawInfo[checksumIndex:])
	if checksum != crc32.ChecksumIEEE(data) {
		logger.Error("check sum does not match")
		return nil, common.ErrChecksumMismatch
	}

	info := sstCodec.decode(data)
	return info, nil
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
	fromKey []byte,
	tableStore *TableStore,
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

func (iter *SSTIterator) Next() (mo.Option[common.KV], error) {
	for {
		entry, err := iter.NextEntry()
		if err != nil {
			return mo.None[common.KV](), err
		}
		keyVal, ok := entry.Get()
		if ok {
			if keyVal.ValueDel.IsTombstone {
				continue
			}

			return mo.Some(common.KV{
				Key:   keyVal.Key,
				Value: keyVal.ValueDel.Value,
			}), nil
		} else {
			return mo.None[common.KV](), nil
		}
	}
}

func (iter *SSTIterator) NextEntry() (mo.Option[common.KVDeletable], error) {
	for {
		if iter.currentBlockIter.IsAbsent() {
			nextBlockIter, err := iter.nextBlockIter()
			if err != nil {
				logger.Error("unable to get next entry", zap.Error(err))
				return mo.None[common.KVDeletable](), err
			}

			if nextBlockIter.IsPresent() {
				iter.currentBlockIter = nextBlockIter
			} else {
				return mo.None[common.KVDeletable](), nil
			}
		}

		currentBlockIter, _ := iter.currentBlockIter.Get()
		kv, err := currentBlockIter.NextEntry()
		if err != nil {
			return mo.None[common.KVDeletable](), err
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

// spawnFetches - Each SST has multiple blocks, this method will create goroutines to fetch blocks within a range
// Range{blocksStart, blocksEnd} for a given SST from object storage
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

		// TODO: ensure goroutine does not leak.
		go func(table *SSTableHandle, store *TableStore, blocksStart uint64, blocksEnd uint64) {
			blocks, err := store.readBlocks(table, common.Range{Start: blocksStart, End: blocksEnd})
			if err != nil {
				blocksCh <- mo.None[[]Block]()
			} else {
				blocksCh <- mo.Some(blocks)
			}
		}(iter.table.clone(), iter.tableStore.clone(), blocksStart, blocksEnd)

		iter.nextBlockIdxToFetch = blocksEnd
	}
}

func (iter *SSTIterator) nextBlockIter() (mo.Option[*BlockIterator], error) {
	for {
		iter.spawnFetches()
		if len(iter.fetchTasks) == 0 {
			common.AssertTrue(int(iter.nextBlockIdxToFetch) == iter.table.info.borrow().BlockMetaLength(), "")
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
			logger.Error("unable to read block")
			return mo.None[*BlockIterator](), common.ErrReadBlocks
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

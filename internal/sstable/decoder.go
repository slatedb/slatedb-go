package sstable

import (
	"encoding/binary"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/gen"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/sstable/bloom"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"go.uber.org/zap"
	"hash/crc32"
)

// SSTableFormat provides helper methods to read byte slice and get SSTable information
// SSTable holds list of blocks(each block is a list of KV pairs) and BloomFilter
// the helper methods mostly help in reading the blocks and the BloomFilter and also to decompress the data
// using the given compress.Codec
//
// TODO(thrawn01): Separate SSTableFormat into Decoder and Config structs
type SSTableFormat struct {
	// size of each block in the SSTable
	BlockSize uint64

	// Write SSTables with a bloom filter if the number of keys in the SSTable is greater than or equal to this value.
	// Reads on small SSTables might be faster without a bloom filter.
	MinFilterKeys uint32

	FilterBitsPerKey uint32

	// defines how SSTable is encoded to byte slice and byte slice decoded to SSTable
	SstCodec SsTableInfoCodec

	// the codec used to compress/decompress SSTable before writing/reading from object storage
	CompressionCodec compress.Codec
}

func DefaultSSTableFormat() *SSTableFormat {
	return &SSTableFormat{
		BlockSize:        4096,
		MinFilterKeys:    0,
		FilterBitsPerKey: 10,
		SstCodec:         FlatBufferSSTableInfoCodec{},
		CompressionCodec: compress.CodecNone,
	}
}

func (f *SSTableFormat) ReadInfo(obj common.ReadOnlyBlob) (*Info, error) {
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

	return DecodeBytesToSSTableInfo(metadataBytes, f.SstCodec)
}

func (f *SSTableFormat) ReadFilter(sstInfo *Info, obj common.ReadOnlyBlob) (mo.Option[bloom.Filter], error) {
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

func (f *SSTableFormat) ReadIndex(info *Info, obj common.ReadOnlyBlob) (*Index, error) {
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

	return &Index{Data: data}, nil
}

func (f *SSTableFormat) ReadIndexRaw(info *Info, sstBytes []byte) (*Index, error) {
	indexBytes := sstBytes[info.IndexOffset : info.IndexOffset+info.IndexLen]

	data, err := compress.Decode(indexBytes, info.CompressionCodec)
	if err != nil {
		return nil, err
	}

	return &Index{Data: data}, nil
}

// getBlockRange returns the (startOffset, endOffset) of the data in ssTable that contains the
// blocks within rng
func (f *SSTableFormat) getBlockRange(rng common.Range, sstInfo *Info, index *flatbuf.SsTableIndex) common.Range {
	blockMetaList := index.UnPack().BlockMeta
	startOffset := blockMetaList[rng.Start].Offset

	endOffset := sstInfo.FilterOffset
	if rng.End < uint64(len(blockMetaList)) {
		endOffset = blockMetaList[rng.End].Offset
	}

	return common.Range{Start: startOffset, End: endOffset}
}

// ReadBlocks reads the complete data required into a byte slice (dataBytes)
// and then breaks the data up into slice of Blocks (decodedBlocks) which is returned
func (f *SSTableFormat) ReadBlocks(
	sstInfo *Info,
	indexData *Index,
	blockRange common.Range,
	obj common.ReadOnlyBlob,
) ([]block.Block, error) {
	index := indexData.SsTableIndex()
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
			logger.Error("unable to Decode block", zap.Error(err))
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
	info *Info,
	indexData *Index,
	blockIndex uint64,
	obj common.ReadOnlyBlob,
) (*block.Block, error) {
	blocks, err := f.ReadBlocks(info, indexData, common.Range{Start: blockIndex, End: blockIndex + 1}, obj)
	if err != nil {
		return nil, err
	}
	block := blocks[0]
	return &block, nil
}

func (f *SSTableFormat) ReadBlockRaw(
	info *Info,
	indexData *Index,
	blockIndex uint64,
	sstBytes []byte,
) (*block.Block, error) {
	index := indexData.SsTableIndex()
	blockRange := f.getBlockRange(common.Range{Start: blockIndex, End: blockIndex + 1}, info, index)
	return f.decodeBytesToBlock(sstBytes[blockRange.Start:blockRange.End], info.CompressionCodec)
}

func (f *SSTableFormat) TableBuilder() *Builder {
	return NewBuilder(
		f.BlockSize,
		f.MinFilterKeys,
		f.SstCodec,
		f.FilterBitsPerKey,
		f.CompressionCodec,
	)
}

func (f *SSTableFormat) Clone() *SSTableFormat {
	return &SSTableFormat{
		BlockSize:        f.BlockSize,
		MinFilterKeys:    f.MinFilterKeys,
		CompressionCodec: f.CompressionCodec,
	}
}

package sstable

import (
	"encoding/binary"
	"fmt"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/sstable/bloom"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"go.uber.org/zap"
)

func DefaultConfig() Config {
	return Config{
		BlockSize:        4096,
		MinFilterKeys:    0,
		FilterBitsPerKey: 10,
		Compression:      compress.CodecNone,
	}
}

func ReadInfo(obj common.ReadOnlyBlob) (*Info, error) {
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

	return DecodeInfo(metadataBytes)
}

func ReadFilter(sstInfo *Info, obj common.ReadOnlyBlob) (mo.Option[bloom.Filter], error) {
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

func ReadIndex(info *Info, obj common.ReadOnlyBlob) (*Index, error) {
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

func ReadIndexRaw(info *Info, sstBytes []byte) (*Index, error) {
	indexBytes := sstBytes[info.IndexOffset : info.IndexOffset+info.IndexLen]

	data, err := compress.Decode(indexBytes, info.CompressionCodec)
	if err != nil {
		return nil, err
	}

	return &Index{Data: data}, nil
}

// getBlockRange returns the (startOffset, endOffset) of the data in ssTable that contains the
// blocks within rng
func getBlockRange(rng common.Range, sstInfo *Info, index *Index) common.Range {
	blockMetaList := index.BlockMeta()
	startOffset := blockMetaList[rng.Start].Offset

	endOffset := sstInfo.FilterOffset
	if rng.End < uint64(len(blockMetaList)) {
		endOffset = blockMetaList[rng.End].Offset
	}

	return common.Range{Start: startOffset, End: endOffset}
}

// ReadBlocks reads the complete data required into a byte slice (dataBytes)
// and then breaks the data up into slice of Blocks (decodedBlocks) which is returned
func ReadBlocks(
	sstInfo *Info,
	index *Index,
	blockRange common.Range,
	obj common.ReadOnlyBlob,
) ([]block.Block, error) {
	//index := indexData.SsTableIndex()
	common.AssertTrue(blockRange.Start <= blockRange.End, "block start index cannot be greater than end index")
	common.AssertTrue(blockRange.End <= uint64(index.BlockMetaLength()), "block end index out of range")

	if blockRange.Start == blockRange.End {
		return []block.Block{}, nil
	}

	rng := getBlockRange(blockRange, sstInfo, index)
	dataBytes, err := obj.ReadRange(rng)
	if err != nil {
		logger.Error("unable to read block data", zap.Error(err))
		return nil, err
	}

	startOffset := rng.Start
	decodedBlocks := make([]block.Block, 0)
	blockMetaList := index.BlockMeta()
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

		var decodedBlock block.Block
		if err := block.Decode(&decodedBlock, blockBytes, compressionCodec); err != nil {
			logger.Error("unable to Decode block", zap.Error(err))
			return nil, err
		}
		decodedBlocks = append(decodedBlocks, decodedBlock)
	}
	return decodedBlocks, nil
}

// TODO(thrawn01): Remove this, it's not used anywhere
func readBlock(
	info *Info,
	indexData *Index,
	blockIndex uint64,
	obj common.ReadOnlyBlob,
) (*block.Block, error) {
	blocks, err := ReadBlocks(info, indexData, common.Range{Start: blockIndex, End: blockIndex + 1}, obj)
	if err != nil {
		return nil, err
	}
	block := blocks[0]
	return &block, nil
}

func ReadBlockRaw(
	info *Info,
	index *Index,
	blockIndex uint64,
	sstBytes []byte,
) (*block.Block, error) {
	blockRange := getBlockRange(common.Range{Start: blockIndex, End: blockIndex + 1}, info, index)

	fmt.Printf("%+v\n", blockRange)
	var blk block.Block
	if err := block.Decode(&blk, sstBytes[blockRange.Start:blockRange.End], info.CompressionCodec); err != nil {
		return nil, err
	}
	return &blk, nil
}

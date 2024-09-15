package internal

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	flatbuf "github.com/naveen246/slatedb-go/gen"
	"github.com/samber/mo"
)

// ------------------------------------------------
// SsTableFormat
// ------------------------------------------------

type SsTableFormat struct {
	blockSize        uint64
	minFilterKeys    uint32
	compressionCodec CompressionCodec
}

func newSsTableFormat(blockSize uint64, minFilterKeys uint32, compression CompressionCodec) *SsTableFormat {
	return &SsTableFormat{
		blockSize:        blockSize,
		minFilterKeys:    minFilterKeys,
		compressionCodec: compression,
	}
}

func (f *SsTableFormat) readInfo(obj ReadOnlyBlob) (*flatbuf.SsTableInfo, error) {
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

	return decodeBytesToSsTableInfo(metadataBytes)
}

func (f *SsTableFormat) readFilter(sstInfo *flatbuf.SsTableInfo, obj ReadOnlyBlob) (mo.Option[BloomFilter], error) {
	if sstInfo.FilterLen() < 1 {
		return mo.None[BloomFilter](), nil
	}

	filterOffsetRange := Range{sstInfo.FilterOffset(), sstInfo.FilterOffset() + sstInfo.FilterLen()}
	filterBytes, err := obj.readRange(filterOffsetRange)
	if err != nil {
		return mo.None[BloomFilter](), err
	}

	filter := decodeBytesToBloomFilter(filterBytes)
	return mo.Some(filter), nil
}

// decompress the compressed data using the specified compression codec.
// TODO: implement more compression options
func (f *SsTableFormat) decompress(compressedData []byte, compression CompressionCodec) ([]byte, error) {
	switch compression {
	case CompressionNone:
		return compressedData, nil
	case CompressionSnappy:
		return snappy.Decode(nil, compressedData)
	case CompressionZlib:
		r, err := zlib.NewReader(bytes.NewReader(compressedData))
		if err != nil {
			return []byte{}, err
		}
		defer r.Close()
		return io.ReadAll(r)
	}

	return []byte{}, ErrInvalidCompressionCodec
}

// getBlockRange returns the (startOffset, endOffset) of the data in ssTable that contains the
// blocks within blockRange
func (f *SsTableFormat) getBlockRange(blockRange Range, sstInfo *flatbuf.SsTableInfo) Range {
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
func (f *SsTableFormat) readBlocks(
	sstInfo *flatbuf.SsTableInfo,
	blockRange Range,
	obj ReadOnlyBlob,
) ([]Block, error) {
	assertTrue(blockRange.start <= blockRange.end, "block start index cannot be greater than end index")
	assertTrue(blockRange.end <= uint64(sstInfo.BlockMetaLength()), "block end index out of range")

	if blockRange.start == blockRange.end {
		return []Block{}, nil
	}

	r := f.getBlockRange(blockRange, sstInfo)
	dataBytes, err := obj.readRange(r)
	if err != nil {
		return []Block{}, err
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
			return []Block{}, err
		}
		decodedBlocks = append(decodedBlocks, decodedBlock)
	}
	return decodedBlocks, nil
}

func (f *SsTableFormat) decodeBytesToBlock(bytes []byte) (Block, error) {
	// last 4 bytes hold the checksum
	checksumIndex := len(bytes) - SizeOfUint32InBytes
	blockBytes := bytes[:checksumIndex]
	checksum := binary.BigEndian.Uint32(bytes[checksumIndex:])
	if checksum != crc32.ChecksumIEEE(blockBytes) {
		return Block{}, ErrChecksumMismatch
	}

	decodedBlock := decodeBytesToBlock(blockBytes)
	decompressedBytes, err := f.decompress(decodedBlock.data, f.compressionCodec)
	if err != nil {
		return Block{}, err
	}

	return Block{
		data:    decompressedBytes,
		offsets: decodedBlock.offsets,
	}, nil
}

func (f *SsTableFormat) readBlock(
	sstInfo *flatbuf.SsTableInfo,
	blockIndex uint64,
	obj ReadOnlyBlob,
) (Block, error) {
	blocks, err := f.readBlocks(sstInfo, Range{blockIndex, blockIndex + 1}, obj)
	if err != nil {
		return Block{}, err
	}
	return blocks[0], nil
}

func (f *SsTableFormat) readBlockRaw(
	sstInfo *flatbuf.SsTableInfo,
	blockIndex uint64,
	sstBytes []byte,
) (Block, error) {
	blockRange := f.getBlockRange(Range{blockIndex, blockIndex + 1}, sstInfo)
	return f.decodeBytesToBlock(sstBytes[blockRange.start:blockRange.end])
}

func (f *SsTableFormat) tableBuilder() *EncodedSsTableBuilder {
	return newEncodedSsTableBuilder(f.blockSize, f.minFilterKeys, f.compressionCodec)
}

// ------------------------------------------------
// EncodedSsTable
// ------------------------------------------------

type EncodedSsTable struct {
	sstInfo          flatbuf.SsTableInfo
	filter           mo.Option[BloomFilter]
	unconsumedBlocks []byte
}

type EncodedSsTableBuilder struct {
}

func newEncodedSsTableBuilder(
	blockSize uint64,
	minFilterKeys uint32,
	compressionCodec CompressionCodec,
) *EncodedSsTableBuilder {
	return &EncodedSsTableBuilder{}
}

func encodeSsTableInfo(sstInfo *flatbuf.SsTableInfo, buf []byte) {
	buf = append(buf, sstInfo.Table().Bytes...)
	binary.BigEndian.PutUint32(buf, crc32.ChecksumIEEE(sstInfo.Table().Bytes))
}

func decodeBytesToSsTableInfo(rawBlockMeta []byte) (*flatbuf.SsTableInfo, error) {
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

	return flatbuf.GetRootAsSsTableInfo(data, 0), nil
}

package sstable

import (
	"bytes"
	"encoding/binary"
	"github.com/gammazero/deque"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/gen"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/sstable/bloom"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"hash/crc32"
)

// Table is the in memory representation of an SSTable
type Table struct {
	Info *Info

	Bloom mo.Option[bloom.Filter]

	// Blocks is a list of blocks contained in the Table
	// NOTE: The final block added to the queue includes
	// the encoded form of sstable.Index and sstable.Info
	Blocks *deque.Deque[[]byte]
}

// Builder builds the SSTable in the format outlined
// in the diagram below.
//
// +-----------------------------------------------+
// |               SSTable                         |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  List of Blocks                         |  |
// |  |  +-----------------------------------+  |  |
// |  |  |  block.Block                      |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  List of KeyValue pairs        |  |  |
// |  |  |  |  +---------------------------+ |  |  |
// |  |  |  |  |  Key Length (2 bytes)     | |  |  |
// |  |  |  |  |  Key                      | |  |  |
// |  |  |  |  |  Value Length (4 bytes)   | |  |  |
// |  |  |  |  |  Value                    | |  |  |
// |  |  |  |  +---------------------------+ |  |  |
// |  |  |  |  ...                           |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Offsets for each Key          |  |  |
// |  |  |  |  (n * 2 bytes)                 |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Number of Offsets (2 bytes)   |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Checksum (4 bytes)            |  |  |
// |  |  +-----------------------------------+  |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  bloom.Filter (if MinFilterKeys met)    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  flatbuf.SsTableIndexT                  |  |
// |  |  (List of Block Offsets)                |  |
// |  |  - Block Offset (Start of Block)        |  |
// |  |  - FirstKey of this Block               |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  flatbuf.SsTableInfoT                   |  |
// |  |  - FirstKey of the SSTable              |  |
// |  |  - Offset of bloom.Filter               |  |
// |  |  - Length of bloom.Filter               |  |
// |  |  - Offset of flatbuf.SsTableIndexT      |  |
// |  |  - Length of flatbuf.SsTableIndexT      |  |
// |  |  - The Compression Codec                |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  Offset of SsTableInfoT (4 bytes)       |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
type Builder struct {
	blockBuilder  *block.Builder
	filterBuilder *bloom.Builder

	// The metadata for each block held by the SSTableIndex
	blockMetaList []*flatbuf.BlockMetaT

	// FirstKey is the first key of the current block that is being built.
	// When the current block reaches BlockSize, a new Block is created and
	// firstKey will then hold the first key of the new block
	firstKey mo.Option[[]byte]

	// sstFirstKey is the first key of the first block in the SSTable
	sstFirstKey mo.Option[[]byte]

	// The encoded/serialized blocks that get added to the SSTable
	blocks *deque.Deque[[]byte]

	blockSize uint64

	// currentLen is the total length of all existing blocks
	currentLen uint64

	// if numKeys >= minFilterKeys then we add a BloomFilter
	// else we don't add BloomFilter since reading smaller set of keys
	// is likely faster without BloomFilter
	minFilterKeys uint32
	numKeys       uint32

	sstCodec         SsTableInfoCodec
	compressionCodec compress.Codec
}

// Config specifies how SSTable is Encoded and Decoded
type Config struct {
	// BlockSize is the size of each block in the SSTable
	BlockSize uint64

	// MinFilterKeys is the minimum number of keys that must exist in the SSTable
	// before a bloom filter is created. Reads on SSTables with a small number
	// of items is faster than looking up in a bloom filter.
	MinFilterKeys uint32

	FilterBitsPerKey uint32

	// The codec used to compress new SSTables. The compression codec used in
	// existing SSTables already written disk is encoded into the SSTableInfo and
	// will be used when decompressing the blocks in that SSTable.
	Compression compress.Codec
}

// NewBuilder create a builder
func NewBuilder(
// TODO(thrawn01): use Config
	blockSize uint64,
	minFilterKeys uint32,
	sstCodec SsTableInfoCodec,
	filterBitsPerKey uint32,
	compressionCodec compress.Codec,
) *Builder {
	return &Builder{
		filterBuilder:    bloom.NewBuilder(filterBitsPerKey),
		blockBuilder:     block.NewBuilder(blockSize),
		blocks:           deque.New[[]byte](0),
		blockMetaList:    []*flatbuf.BlockMetaT{},
		compressionCodec: compressionCodec,
		firstKey:         mo.None[[]byte](),
		sstFirstKey:      mo.None[[]byte](),
		minFilterKeys:    minFilterKeys,
		blockSize:        blockSize,
		sstCodec:         sstCodec,
		currentLen:       0,
		numKeys:          0,
	}
}

func (b *Builder) Add(key []byte, value mo.Option[[]byte]) error {
	b.numKeys += 1

	if !b.blockBuilder.Add(key, value) {
		// Create a new block builder and append block data
		blockBytes, err := b.finishBlock()
		if err != nil {
			return err
		}
		buf, ok := blockBytes.Get()
		if ok {
			b.currentLen += uint64(len(buf))
			b.blocks.PushBack(buf)
		}

		addSuccess := b.blockBuilder.Add(key, value)
		common.AssertTrue(addSuccess, "block.Builder.Add() failed")
		b.firstKey = mo.Some(key)
	} else if b.sstFirstKey.IsAbsent() {
		b.sstFirstKey = mo.Some(key)
		b.firstKey = mo.Some(key)
	}

	b.filterBuilder.Add(key)
	return nil
}

func (b *Builder) NextBlock() mo.Option[[]byte] {
	if b.blocks.Len() == 0 {
		return mo.None[[]byte]()
	}
	return mo.Some(b.blocks.PopFront())
}

func (b *Builder) finishBlock() (mo.Option[[]byte], error) {
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

	buf := make([]byte, 0, len(compressedBlock)+common.SizeOfUint32)
	buf = append(buf, compressedBlock...)
	buf = binary.BigEndian.AppendUint32(buf, checksum)

	return mo.Some(buf), nil
}

func (b *Builder) Build() (*Table, error) {
	blkBytes, err := b.finishBlock()
	if err != nil {
		return nil, err
	}
	buf, ok := blkBytes.Get()
	if !ok {
		buf = []byte{}
	}

	// Write the filter if the total number of keys equals of exceeds minFilterKeys
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

	// Write the index block
	sstIndex := flatbuf.SsTableIndexT{BlockMeta: b.blockMetaList}
	indexBlock := FlatBufferSSTableIndexCodec{}.Encode(sstIndex)
	compressedIndexBlock, err := compress.Encode(indexBlock, b.compressionCodec)
	if err != nil {
		return nil, err
	}
	indexOffset := b.currentLen + uint64(len(buf))
	buf = append(buf, compressedIndexBlock...)

	metaOffset := b.currentLen + uint64(len(buf))
	firstKey, _ := b.sstFirstKey.Get()

	sstInfo := &Info{
		FirstKey:         bytes.Clone(firstKey),
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

	return &Table{
		Info:   sstInfo,
		Bloom:  maybeFilter,
		Blocks: b.blocks,
	}, nil
}

// EncodeTable encodes the provided sstable.Table into the
// SSTable format as []byte.
func EncodeTable(table *Table) []byte {
	var result []byte
	for i := 0; i < table.Blocks.Len(); i++ {
		result = append(result, table.Blocks.At(i)...)
	}
	return result
}

// TODO(thrawn01): Rename this to sstable.decode which is only used by SSTableFormat
//  which should be renamed to sstable.Decoder
func DecodeBytesToSSTableInfo(rawInfo []byte, sstCodec SsTableInfoCodec) (*Info, error) {
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

package sstable

import (
	"bytes"
	"encoding/binary"
	"github.com/gammazero/deque"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/flatbuf"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/sstable/bloom"
	"github.com/slatedb/slatedb-go/internal/types"
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
// |  |  Checksum of SsTableInfoT (4 bytes)     |  |
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

	// firstKey is the first key of the first block in the SSTable
	firstKey mo.Option[[]byte]

	// The encoded/serialized blocks that get added to the SSTable
	blocks *deque.Deque[[]byte]

	// currentLen is the total length of all existing blocks
	currentLen uint64

	// if numKeys >= minFilterKeys then we add a BloomFilter
	// else we don't add BloomFilter since reading smaller set of keys
	// is likely faster without BloomFilter
	numKeys uint32

	// config is the config options used to build the SSTable
	conf Config
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
func NewBuilder(conf Config) *Builder {
	return &Builder{
		filterBuilder: bloom.NewBuilder(conf.FilterBitsPerKey),
		blockBuilder:  block.NewBuilder(conf.BlockSize),
		blocks:        deque.New[[]byte](0),
		blockMetaList: []*flatbuf.BlockMetaT{},
		firstKey:      mo.None[[]byte](),
		conf:          conf,
		currentLen:    0,
		numKeys:       0,
	}
}

func (b *Builder) AddValue(key []byte, value []byte) error {
	// TODO(thrawn01): As of now, all of the code assumes if the value is missing it is
	//  a tombstone. Once we implement transactions we should remove AddValue() method and
	//  explicitly set the types.RowEntry.Value.Kind to determine what kind of value it is,
	//  instead of assuming it is a tombstone if the value is absent.
	if len(value) == 0 {
		return b.Add(key, types.RowEntry{Value: types.Value{Kind: types.KindTombStone}})
	}
	return b.Add(key, types.RowEntry{Value: types.Value{Value: value}})
}

func (b *Builder) Add(key []byte, entry types.RowEntry) error {
	b.numKeys += 1
	row := block.Row{Value: entry.Value}

	if !b.blockBuilder.Add(key, row) {
		// Create a new block builder and append block data
		buf, err := b.finishBlock()
		if err != nil {
			return err
		}
		b.currentLen += uint64(len(buf))
		b.blocks.PushBack(buf)

		addSuccess := b.blockBuilder.Add(key, row)
		assert.True(addSuccess, "block.Builder.AddValue() failed")
	}

	if b.firstKey.IsAbsent() {
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

func (b *Builder) finishBlock() ([]byte, error) {
	if b.blockBuilder.IsEmpty() {
		return nil, nil
	}

	blockBuilder := b.blockBuilder
	b.blockBuilder = block.NewBuilder(b.conf.BlockSize)
	blk, err := blockBuilder.Build()
	if err != nil {
		return nil, err
	}

	buf, err := block.Encode(blk, b.conf.Compression)
	if err != nil {
		return nil, err
	}

	blockMeta := flatbuf.BlockMetaT{Offset: b.currentLen, FirstKey: blk.FirstKey}
	b.blockMetaList = append(b.blockMetaList, &blockMeta)

	return buf, nil
}

func (b *Builder) Build() (*Table, error) {
	buf, err := b.finishBlock()
	if err != nil {
		return nil, err
	}

	// Write the filter if the total number of keys equals of exceeds minFilterKeys
	maybeFilter := mo.None[bloom.Filter]()
	filterLen := 0
	filterOffset := b.currentLen + uint64(len(buf))
	if b.numKeys >= b.conf.MinFilterKeys {
		filter := b.filterBuilder.Build()
		compressedFilter, err := compress.Encode(bloom.Encode(filter), b.conf.Compression)
		if err != nil {
			return nil, err
		}
		filterLen = len(compressedFilter)
		buf = append(buf, compressedFilter...)
		maybeFilter = mo.Some(filter)
	}

	// Compress and Write the index block
	sstIndex := flatbuf.SsTableIndexT{BlockMeta: b.blockMetaList}
	indexBlock := encodeIndex(sstIndex)
	compressedIndexBlock, err := compress.Encode(indexBlock, b.conf.Compression)
	if err != nil {
		return nil, err
	}
	indexOffset := b.currentLen + uint64(len(buf))
	buf = append(buf, compressedIndexBlock...)

	metaOffset := b.currentLen + uint64(len(buf))
	firstKey, _ := b.firstKey.Get()

	// Append the encoded Info and checksum
	sstInfo := &Info{
		FirstKey:         bytes.Clone(firstKey),
		IndexOffset:      indexOffset,
		IndexLen:         uint64(len(compressedIndexBlock)),
		FilterOffset:     filterOffset,
		FilterLen:        uint64(filterLen),
		CompressionCodec: b.conf.Compression,
	}
	buf = append(buf, EncodeInfo(sstInfo)...)

	// write the metadata offset at the end of the file.
	buf = binary.BigEndian.AppendUint32(buf, uint32(metaOffset))
	b.blocks.PushBack(buf)

	return &Table{
		Info:   sstInfo,
		Bloom:  maybeFilter,
		Blocks: b.blocks,
	}, nil
}

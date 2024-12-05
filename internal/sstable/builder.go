package sstable

import (
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

// EncodedSSTable is the in memory representation of an SSTable
// TODO(thrawn01): Rename this to sstable.Encoded or sstable.Table
type EncodedSSTable struct {
	Info *Info

	Bloom mo.Option[bloom.Filter]

	// Blocks is a list of blocks contained in the EncodedSSTable
	Blocks *deque.Deque[[]byte]
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

	sstCodec         SsTableInfoCodec
	compressionCodec compress.Codec
}

// Create a builder based on target block size.
func NewEncodedSSTableBuilder(
	blockSize uint64,
	minFilterKeys uint32,
	sstCodec SsTableInfoCodec,
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

func (b *EncodedSSTableBuilder) Add(key []byte, value mo.Option[[]byte]) error {
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
		common.AssertTrue(addSuccess, "block.Builder Add failed")
		b.firstKey = mo.Some(key)
	} else if b.sstFirstKey.IsAbsent() {
		b.sstFirstKey = mo.Some(key)
		b.firstKey = mo.Some(key)
	}

	b.filterBuilder.Add(key)
	return nil
}

func (b *EncodedSSTableBuilder) NextBlock() mo.Option[[]byte] {
	if b.blocks.Len() == 0 {
		return mo.None[[]byte]()
	}
	block := b.blocks.PopFront()
	return mo.Some(block)
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

func (b *EncodedSSTableBuilder) Build() (*EncodedSSTable, error) {
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
	indexBlock := FlatBufferSSTableIndexCodec{}.Encode(sstIndex)
	compressedIndexBlock, err := compress.Encode(indexBlock, b.compressionCodec)
	if err != nil {
		return nil, err
	}
	indexOffset := b.currentLen + uint64(len(buf))
	buf = append(buf, compressedIndexBlock...)

	metaOffset := b.currentLen + uint64(len(buf))
	sstInfo := &Info{
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
		Info:   sstInfo,
		Bloom:  maybeFilter,
		Blocks: b.blocks,
	}, nil
}

// TODO(thrawn01): Rename this to sstable.Decode ?
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

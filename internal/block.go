package internal

import (
	"encoding/binary"
	"github.com/samber/mo"
	"math"
)

const (
	SizeOfUint16 = 2
	SizeOfUint32 = 4

	Tombstone = math.MaxUint32
)

// ------------------------------------------------
// Block
// ------------------------------------------------

type Block struct {
	data    []byte
	offsets []uint16
}

// encode converts Block to a byte slice
// data is added to the first len(data) bytes
// offsets are added to the next len(offsets) * SizeOfUint16 bytes
// the last SizeOfUint16 bytes hold the offset count
func (b *Block) encode() []byte {
	bufSize := len(b.data) + len(b.offsets)*SizeOfUint16 + SizeOfUint16

	buf := make([]byte, 0, bufSize)
	buf = append(buf, b.data...)
	for _, offset := range b.offsets {
		buf = binary.BigEndian.AppendUint16(buf, offset)
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(b.offsets)))
	return buf
}

// decode converts byte slice to a Block
func decodeBytesToBlock(bytes []byte) Block {
	offsetCount := binary.BigEndian.Uint16(bytes[len(bytes)-SizeOfUint16:])
	dataEndIndex := len(bytes) - SizeOfUint16 - (int(offsetCount) * SizeOfUint16)

	offsets := make([]uint16, 0, offsetCount)
	for i := 0; i < int(offsetCount); i++ {
		index := dataEndIndex + (i * SizeOfUint16)
		offsets = append(offsets, binary.BigEndian.Uint16(bytes[index:]))
	}

	return Block{
		data:    bytes[:dataEndIndex],
		offsets: offsets,
	}
}

// ------------------------------------------------
// BlockBuilder
// ------------------------------------------------

type BlockBuilder struct {
	offsets   []uint16
	data      []byte
	blockSize uint
}

func NewBlockBuilder(blockSize uint) BlockBuilder {
	return BlockBuilder{
		offsets:   make([]uint16, 0),
		data:      make([]byte, 0),
		blockSize: blockSize,
	}
}

func (b *BlockBuilder) estimatedSize() int {
	return SizeOfUint16 + // number of key-value pairs in the block
		(len(b.offsets) * SizeOfUint16) + // offsets
		len(b.data) // key-value pairs
}

func (b *BlockBuilder) add(key []byte, value mo.Option[[]byte]) bool {
	if len(key) == 0 {
		panic("key must not be empty")
	}

	valueLen := 0
	val, ok := value.Get()
	if ok {
		valueLen = len(val)
	}
	newSize := b.estimatedSize() + len(key) + valueLen + (SizeOfUint16 * 2) + SizeOfUint32

	// If adding the key-value pair would exceed the block size limit, don't add it.
	// (Unless the block is empty, in which case, allow the block to exceed the limit.)
	if uint(newSize) > b.blockSize && !b.isEmpty() {
		return false
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))
	b.data = binary.BigEndian.AppendUint16(b.data, uint16(len(key)))
	b.data = append(b.data, key...)
	if valueLen > 0 {
		b.data = binary.BigEndian.AppendUint32(b.data, uint32(valueLen))
		b.data = append(b.data, val...)
	} else {
		b.data = binary.BigEndian.AppendUint32(b.data, Tombstone)
	}

	return true
}

func (b *BlockBuilder) isEmpty() bool {
	return len(b.offsets) == 0
}

func (b *BlockBuilder) build() (*Block, error) {
	if b.isEmpty() {
		return nil, EmptyBlock
	}
	return &Block{
		data:    b.data,
		offsets: b.offsets,
	}, nil
}

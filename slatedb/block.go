package slatedb

import (
	"bytes"
	"encoding/binary"
	"github.com/naveen246/slatedb-go/slatedb/common"
	"math"

	"github.com/samber/mo"
)

const (
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
// offsets are added to the next len(offsets) * SizeOfUint16InBytes bytes
// the last 2 bytes hold the number of offsets
func (b *Block) encodeToBytes() []byte {
	bufSize := len(b.data) + len(b.offsets)*common.SizeOfUint16InBytes + common.SizeOfUint16InBytes

	buf := make([]byte, 0, bufSize)
	buf = append(buf, b.data...)

	for _, offset := range b.offsets {
		buf = binary.BigEndian.AppendUint16(buf, offset)
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(b.offsets)))
	return buf
}

// decode converts byte slice to a Block
func decodeBytesToBlock(bytes []byte) *Block {
	// the last 2 bytes hold the offset count
	offsetCountIndex := len(bytes) - common.SizeOfUint16InBytes
	offsetCount := binary.BigEndian.Uint16(bytes[offsetCountIndex:])

	offsetStartIndex := offsetCountIndex - (int(offsetCount) * common.SizeOfUint16InBytes)
	offsets := make([]uint16, 0, offsetCount)

	for i := 0; i < int(offsetCount); i++ {
		index := offsetStartIndex + (i * common.SizeOfUint16InBytes)
		offsets = append(offsets, binary.BigEndian.Uint16(bytes[index:]))
	}

	return &Block{
		data:    bytes[:offsetStartIndex],
		offsets: offsets,
	}
}

// ------------------------------------------------
// BlockBuilder
// ------------------------------------------------

type BlockBuilder struct {
	offsets   []uint16
	data      []byte
	blockSize uint64
}

func newBlockBuilder(blockSize uint64) *BlockBuilder {
	return &BlockBuilder{
		offsets:   make([]uint16, 0),
		data:      make([]byte, 0),
		blockSize: blockSize,
	}
}

func (b *BlockBuilder) estimatedSize() int {
	return common.SizeOfUint16InBytes + // number of key-value pairs in the block
		(len(b.offsets) * common.SizeOfUint16InBytes) + // offsets
		len(b.data) // key-value pairs
}

func (b *BlockBuilder) add(key []byte, value mo.Option[[]byte]) bool {
	common.AssertTrue(len(key) > 0, "key must not be empty")

	valueLen := 0
	val, ok := value.Get()
	if ok {
		valueLen = len(val)
	}
	newSize := b.estimatedSize() + len(key) + valueLen + (common.SizeOfUint16InBytes * 2) + common.SizeOfUint32InBytes

	// If adding the key-value pair would exceed the block size limit, don't add it.
	// (Unless the block is empty, in which case, allow the block to exceed the limit.)
	if uint64(newSize) > b.blockSize && !b.isEmpty() {
		return false
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))

	// If value is present then append KeyLength(uint16), Key, ValueLength(uint32), value.
	// if value is absent then append KeyLength(uint16), Key, Tombstone(uint32)
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
		return nil, common.ErrEmptyBlock
	}
	return &Block{
		data:    b.data,
		offsets: b.offsets,
	}, nil
}

// ------------------------------------------------
// BlockIterator
// ------------------------------------------------

type BlockIterator struct {
	block       *Block
	offsetIndex uint64
}

// newBlockIteratorFromKey Construct a BlockIterator that starts at the given key, or at the first
// key greater than the given key if the exact key given is not in the block.
func newBlockIteratorFromKey(block *Block, key []byte) *BlockIterator {
	data := block.data
	index := len(block.offsets)
	// TODO: Rust implementation uses partition_point() which internally uses binary search
	//  we are doing linear search. See if we can optimize
	for i, offset := range block.offsets {
		off := offset
		keyLen := binary.BigEndian.Uint16(data[off:])
		off += common.SizeOfUint16InBytes
		curKey := data[off : off+keyLen]
		if bytes.Compare(curKey, key) >= 0 {
			index = i
			break
		}
	}
	return &BlockIterator{
		block:       block,
		offsetIndex: uint64(index),
	}
}

func newBlockIteratorFromFirstKey(block *Block) *BlockIterator {
	return &BlockIterator{
		block:       block,
		offsetIndex: 0,
	}
}

func (iter *BlockIterator) Next() (mo.Option[common.KV], error) {
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

func (iter *BlockIterator) NextEntry() (mo.Option[common.KVDeletable], error) {
	keyValue, ok := iter.loadAtCurrentOffset().Get()
	if !ok {
		return mo.None[common.KVDeletable](), nil
	}

	iter.advance()
	return mo.Some(keyValue), nil
}

func (iter *BlockIterator) advance() {
	iter.offsetIndex += 1
}

func (iter *BlockIterator) loadAtCurrentOffset() mo.Option[common.KVDeletable] {
	if iter.offsetIndex >= uint64(len(iter.block.offsets)) {
		return mo.None[common.KVDeletable]()
	}

	data := iter.block.data
	offset := iter.block.offsets[iter.offsetIndex]
	var valueDel common.ValueDeletable

	// Read KeyLength(uint16), Key, (ValueLength(uint32), value)/Tombstone(uint32) from data
	keyLen := binary.BigEndian.Uint16(data[offset:])
	offset += common.SizeOfUint16InBytes

	key := data[offset : offset+keyLen]
	offset += keyLen

	valueLen := binary.BigEndian.Uint32(data[offset:])
	offset += common.SizeOfUint32InBytes

	if valueLen != Tombstone {
		value := data[offset : uint32(offset)+valueLen]
		valueDel = common.ValueDeletable{Value: value, IsTombstone: false}
	} else {
		valueDel = common.ValueDeletable{Value: nil, IsTombstone: true}
	}

	return mo.Some(common.KVDeletable{Key: key, ValueDel: valueDel})
}

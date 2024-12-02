package block

import (
	"bytes"
	"encoding/binary"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"sort"
)

// Iterator iterates through KeyValue pairs present in the Block.
type Iterator struct {
	block       *Block
	offsetIndex uint64
}

// NewIterator constructs a block.Iterator that starts at the beginning of the block
func NewIterator(block *Block) *Iterator {
	return &Iterator{
		block:       block,
		offsetIndex: 0,
	}
}

// NewIteratorAtKey Construct a block.Iterator that starts at the given key, or at the first
// key greater than the given key if the exact key given is not in the block.
func NewIteratorAtKey(block *Block, key []byte) *Iterator {
	data := block.Data
	index := sort.Search(len(block.Offsets), func(i int) bool {
		off := block.Offsets[i]
		keyLen := binary.BigEndian.Uint16(data[off:])
		off += common.SizeOfUint16
		curKey := data[off : off+keyLen]
		return bytes.Compare(curKey, key) >= 0
	})

	return &Iterator{
		block:       block,
		offsetIndex: uint64(index),
	}
}

func (iter *Iterator) Next() (mo.Option[common.KV], error) {
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

func (iter *Iterator) NextEntry() (mo.Option[common.KVDeletable], error) {
	keyValue, ok := iter.loadAtCurrentOffset().Get()
	if !ok {
		return mo.None[common.KVDeletable](), nil
	}

	iter.advance()
	return mo.Some(keyValue), nil
}

func (iter *Iterator) advance() {
	iter.offsetIndex += 1
}

func (iter *Iterator) loadAtCurrentOffset() mo.Option[common.KVDeletable] {
	if iter.offsetIndex >= uint64(len(iter.block.Offsets)) {
		return mo.None[common.KVDeletable]()
	}

	data := iter.block.Data
	offset := iter.block.Offsets[iter.offsetIndex]
	var valueDel common.ValueDeletable

	// Read KeyLength(uint16), Key, (ValueLength(uint32), value)/Tombstone(uint32) from data
	keyLen := binary.BigEndian.Uint16(data[offset:])
	offset += common.SizeOfUint16

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

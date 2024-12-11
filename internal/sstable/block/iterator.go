package block

import (
	"bytes"
	"encoding/binary"
	"github.com/slatedb/slatedb-go/internal/types"
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

func (iter *Iterator) Next() (types.KeyValue, bool) {
	for {
		entry, shouldContinue := iter.NextEntry()
		if !shouldContinue {
			return types.KeyValue{}, false
		}
		if entry.Value.IsTombstone() {
			continue
		}
		return types.KeyValue{
			Key:   entry.Key,
			Value: entry.Value.Value,
		}, true
	}
}

func (iter *Iterator) NextEntry() (types.RowEntry, bool) {
	if iter.offsetIndex >= uint64(len(iter.block.Offsets)) {
		return types.RowEntry{}, false
	}
	var result types.RowEntry

	data := iter.block.Data
	offset := iter.block.Offsets[iter.offsetIndex]

	// Read KeyLength(uint16), Key, (ValueLength(uint32), value)/Tombstone(uint32) from data
	keyLen := binary.BigEndian.Uint16(data[offset:])
	offset += common.SizeOfUint16

	result.Key = data[offset : offset+keyLen]
	offset += keyLen

	valueLen := binary.BigEndian.Uint32(data[offset:])
	offset += common.SizeOfUint32

	if valueLen != Tombstone {
		result.Value = types.Value{
			Value: data[offset : uint32(offset)+valueLen],
			Kind:  types.KindKeyValue,
		}
	} else {
		result.Value = types.Value{
			Kind: types.KindTombStone,
		}
	}

	iter.offsetIndex += 1
	return result, true
}

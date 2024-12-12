package block

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/slatedb/slatedb-go/internal/types"
	"sort"
)

// Iterator iterates through KeyValue pairs present in the Block.
type Iterator struct {
	block       *Block
	offsetIndex uint64
	warn        types.ErrWarn
	firstKey    []byte
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
func NewIteratorAtKey(block *Block, key []byte) (*Iterator, error) {
	if len(block.Offsets) <= 0 {
		return nil, errors.New("number of block.Offsets must be greater than zero")
	}

	// First key in the block is a full key which cannot be corrupt else we lose
	// all key values in the block as they are suffixes of the first key.
	first, err := v0RowCodec.PeekAtKey(block.Data[block.Offsets[0]:])
	if err != nil {
		return nil, fmt.Errorf("while peeking at block.Offset[0]: %w; entire block is lost", err)
	}

	// If the first block is our key, then use that
	if bytes.Compare(first.KeySuffix, key) >= 0 {
		return &Iterator{
			block:       block,
			offsetIndex: uint64(0),
		}, nil
	}

	var warn types.ErrWarn
	index := sort.Search(len(block.Offsets)-1, func(i int) bool {
		if block.Offsets[i+1] > uint16(len(block.Data)) {
			warn.Add("block.Offset[%d] is out of bounds %d", i+1, block.Offsets[i])
			return false
		}
		p, _ := v0RowCodec.PeekAtKey(block.Data[block.Offsets[i+1]:])
		if err != nil {
			warn.Add("while peeking at block.Offset[%d]: %s", i+1, err)
			return false
		}
		return bytes.Compare(p.FullKey(first.KeySuffix), key) >= 0
	})

	return &Iterator{
		block:       block,
		offsetIndex: uint64(index + 1),
	}, warn.If()
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

	data := iter.block.Data
	offset := iter.block.Offsets[iter.offsetIndex]

	r, err := v0RowCodec.Decode(data[offset:])
	if err != nil {
		iter.warn.Add("while decoding at block.Offset[%d]: %s", iter.offsetIndex, err)
		return types.RowEntry{}, false
	}

	if iter.firstKey == nil {
		// TODO(thrawn01): Compaction is doing some thing such that the first key isn't a whole key
		//  and this panics. 1) FullKey should protect itself from this, 2) Figure out what compaction is doing
		//  ^^^^ DO THIS NEXT
		iter.firstKey = r.FullKey(nil)
	}

	iter.offsetIndex += 1
	return types.RowEntry{
		Key:   r.FullKey(iter.firstKey),
		Value: r.ToValue(),
	}, true
}

// Warnings returns types.ErrWarn if there was an error during iteration.
// TODO(thrawn01): ensure all users of Iterator check for warnings
//  also, add tests for blocks with warnings
func (iter *Iterator) Warnings() *types.ErrWarn {
	return &iter.warn
}

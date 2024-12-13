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
	var warn types.ErrWarn

	// First key in the block should be a full key. -- the block.Builder ensures this is true --
	// If it is corrupt we could lose all key values in the block IF they are all suffixes of the
	// first key. As such, we search for the first full key in the block until we find one and begin
	// iteration there. The fast path assumes the first block is valid and is a full key.
	first, idx, ok := firstFullKey(block, &warn)
	if !ok {
		// If we couldn't find a first full key, and there are no warnings
		// we must assume the block is empty or not a block
		if warn.Empty() {
			return nil, fmt.Errorf("corrupt block; no full key found")
		}
		return nil, &warn
	}

	// If the first block is our key, then use that
	if bytes.Equal(first.keySuffix, key) {
		return &Iterator{
			firstKey:    bytes.Clone(first.keySuffix),
			offsetIndex: uint64(0),
			block:       block,
			warn:        warn,
		}, nil
	}

	// Start searching for keys at the first key found; which is idx=0 unless
	// the first key was corrupt.
	index := sort.Search(len(block.Offsets)-idx, func(i int) bool {
		if block.Offsets[i+idx] > uint16(len(block.Data)) {
			warn.Add("block.Offset[%d] = %d is out of bounds", i+idx, block.Offsets[i+idx])
			return false
		}
		p, err := v0RowCodec.PeekAtKey(block.Data[block.Offsets[i+idx]:], first.keySuffix)
		if err != nil {
			warn.Add("while peeking at block.Offset[%d]: %s", i+idx, err)
			return false
		}
		return bytes.Compare(v0FullKey(p, first.keySuffix), key) >= 0
	})

	return &Iterator{
		firstKey:    bytes.Clone(first.keySuffix),
		offsetIndex: uint64(index + idx),
		block:       block,
		warn:        warn,
	}, nil
}

func (iter *Iterator) Next() (types.KeyValue, bool) {
	for {
		entry, ok := iter.NextEntry()
		if !ok {
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

	r, err := v0RowCodec.Decode(data[offset:], iter.firstKey)
	if err != nil {
		iter.warn.Add("while decoding block.Offset[%d]: %s", iter.offsetIndex, err)
		return types.RowEntry{}, false
	}

	if iter.firstKey == nil {
		iter.firstKey = v0FullKey(*r, nil)
	}

	iter.offsetIndex += 1
	return types.RowEntry{
		Key:   v0FullKey(*r, iter.firstKey),
		Value: r.ToValue(),
	}, true
}

// Warnings returns types.ErrWarn if there was an error during iteration.
func (iter *Iterator) Warnings() *types.ErrWarn {
	return &iter.warn
}

// firstFullKey finds the first full key -- which is a key with no keyPrefixLen set -- and
// returns that key, and index found as the first key in the block. If we encounter a corrupted
// key, we consider subsequent keys for the next full key in the block and return that instead.
func firstFullKey(block *Block, warn *types.ErrWarn) (Row, int, bool) {
	for i, offset := range block.Offsets {
		row, err := v0RowCodec.PeekAtKey(block.Data[offset:], nil)
		if err != nil {
			warn.Add("while peeking at key at offset %d: %v", offset, err)
			continue
		}

		if row.keyPrefixLen == 0 {
			return row, i, true
		}
	}

	warn.Add("unable to locate uncorrupted first key in block; block is corrupt")
	return Row{}, 0, false
}

package sstable

import (
	"bytes"
	"context"
	"fmt"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

type TableStore interface {
	ReadIndex(*Handle) (*Index, error)
	ReadBlocksUsingIndex(*Handle, common.Range, *Index) ([]block.Block, error)
}

// Iterator iterates through KeyValue pairs present in the SSTable.
type Iterator struct {
	blockIter *block.Iterator
	warn      types.ErrWarn
	store     TableStore
	handle    *Handle
	index     *Index
	fromKey   []byte
	nextBlock uint64
}

func NewIterator(handle *Handle, store TableStore) (*Iterator, error) {
	index, err := store.ReadIndex(handle)
	if err != nil {
		return nil, err
	}

	return &Iterator{
		handle:    handle,
		store:     store,
		index:     index,
		nextBlock: 0,
	}, nil
}

func NewIteratorAtKey(handle *Handle, key []byte, store TableStore) (*Iterator, error) {
	index, err := store.ReadIndex(handle)
	if err != nil {
		return nil, err
	}

	iter := &Iterator{
		fromKey: bytes.Clone(key),
		handle:  handle,
		store:   store,
		index:   index,
	}
	iter.nextBlock = iter.firstBlockIncludingOrAfterKey(index, key)
	return iter, nil
}

func (iter *Iterator) Next(ctx context.Context) (types.KeyValue, bool) {
	for {
		keyVal, ok := iter.NextEntry(ctx)
		if !ok {
			return types.KeyValue{}, false
		}

		if keyVal.Value.IsTombstone() {
			continue
		}

		return types.KeyValue{
			Key:   keyVal.Key,
			Value: keyVal.Value.Value,
		}, true
	}
}

func (iter *Iterator) NextEntry(ctx context.Context) (types.RowEntry, bool) {
	for {
		if iter.blockIter == nil {
			it, err := iter.nextBlockIter()
			if err != nil {
				// TODO(thrawn01): This could be a transient error, or a corruption error
				//  we need to handle each differently.
				iter.warn.Add("while fetching blocks for SST '%s': %s",
					iter.handle.Id.String(), err.Error())
				return types.RowEntry{}, false
			}
			if it == nil { // No more blocks
				return types.RowEntry{}, false
			}
			iter.blockIter = it
		}

		kv, ok := iter.blockIter.NextEntry(ctx)
		if !ok {
			if warn := iter.blockIter.Warnings(); warn != nil {
				iter.warn.Merge(warn)
			}
			// We have exhausted the current block, but not necessarily the entire SST,
			// so we fall back to the top to check if we have more blocks to read.
			iter.blockIter = nil
			continue
		}

		return kv, true
	}
}

// nextBlockIter fetches the next block and returns an iterator for that block
func (iter *Iterator) nextBlockIter() (*block.Iterator, error) {
	if iter.nextBlock >= uint64(iter.index.BlockMetaLength()) {
		return nil, nil // No more blocks to read
	}

	// Fetch the next block
	rng := common.Range{Start: iter.nextBlock, End: iter.nextBlock + 1}
	blocks, err := iter.store.ReadBlocksUsingIndex(iter.handle, rng, iter.index)
	if err != nil {
		return nil, fmt.Errorf("while reading block range [%d:%d]: %w", rng.Start, rng.End, err)
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("block read range [%d:%d] returned zero blocks", rng.Start, rng.End)
	}

	// Increment the iter.nextBlock
	iter.nextBlock++

	// If iter.fromKey is present use NewIteratorAtKey() to find the key in the block
	if iter.fromKey != nil {
		// Will return an iterator nearest to where the key should be if it doesn't exist.
		return block.NewIteratorAtKey(&blocks[0], iter.fromKey)
	}

	// Iterate through all the blocks
	return block.NewIterator(&blocks[0]), nil
}

// firstBlockIncludingOrAfterKey performs a binary search on the SSTable index to find the first block
// that either includes the given key or is the first block after the key. This ensures we start reading
// from either the block containing the key or the first block that could contain keys greater than the search key.
func (iter *Iterator) firstBlockIncludingOrAfterKey(index *Index, key []byte) uint64 {
	low := 0
	high := index.BlockMetaLength() - 1
	foundBlockID := 0

loop:
	for low <= high {
		mid := low + (high-low)/2
		// Compare the middle block's first key with the search key.
		midBlockFirstKey := index.BlockMeta()[mid].FirstKey
		cmp := bytes.Compare(midBlockFirstKey, key)
		switch cmp {
		// If the search key is greater, narrow the search to the upper half.
		case -1: // key > midBlockFirstKey
			low = mid + 1
			foundBlockID = mid
		// If the search key is smaller, narrow the search to the lower half.
		case 1: // key < midBlockFirstKey
			if mid > 0 {
				high = mid - 1
			} else {
				break loop
			}
		// If they're equal, we've found the exact block, return its index.
		case 0: // exact match
			return uint64(mid)
		}
	}

	return uint64(foundBlockID)
}

// Warnings returns types.ErrWarn if there was a warning during iteration.
func (iter *Iterator) Warnings() *types.ErrWarn {
	return &iter.warn
}

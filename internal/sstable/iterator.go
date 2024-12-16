package sstable

import (
	"bytes"
	"fmt"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"math"
	"sync"
)

type TableStore interface {
	ReadIndex(*Handle) (*Index, error)
	ReadBlocksUsingIndex(*Handle, common.Range, *Index) ([]block.Block, error)
}

// Iterator helps in iterating through KeyValue pairs present in the SSTable.
// Since each SSTable is a list of Blocks, this iterator internally uses block.Iterator to iterate through each Block
type Iterator struct {
	table               *Handle
	indexData           *Index
	tableStore          TableStore
	currentBlockIter    mo.Option[*block.Iterator]
	fromKey             mo.Option[[]byte]
	nextBlockIdxToFetch uint64
	fetchTasks          chan chan mo.Option[[]block.Block]
	maxFetchTasks       uint64
	numBlocksToFetch    uint64
	warn                types.ErrWarn
}

func NewIterator(
	table *Handle,
	tableStore TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
) (*Iterator, error) {
	indexData, err := tableStore.ReadIndex(table)
	if err != nil {
		return nil, err
	}

	return &Iterator{
		table:               table,
		indexData:           indexData,
		tableStore:          tableStore,
		currentBlockIter:    mo.None[*block.Iterator](),
		fromKey:             mo.None[[]byte](),
		nextBlockIdxToFetch: 0,
		fetchTasks:          make(chan chan mo.Option[[]block.Block], maxFetchTasks),
		maxFetchTasks:       maxFetchTasks,
		numBlocksToFetch:    numBlocksToFetch,
	}, nil
}

func NewIteratorAtKey(
	table *Handle,
	fromKey []byte,
	tableStore TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
) (*Iterator, error) {
	indexData, err := tableStore.ReadIndex(table)
	if err != nil {
		return nil, err
	}

	iter := &Iterator{
		table:            table,
		indexData:        indexData,
		tableStore:       tableStore,
		currentBlockIter: mo.None[*block.Iterator](),
		fromKey:          mo.Some(fromKey),
		fetchTasks:       make(chan chan mo.Option[[]block.Block], maxFetchTasks),
		maxFetchTasks:    maxFetchTasks,
		numBlocksToFetch: numBlocksToFetch,
	}
	iter.nextBlockIdxToFetch = iter.firstBlockWithDataIncludingOrAfterKey(indexData, fromKey)
	return iter, nil
}

func (iter *Iterator) Next() (types.KeyValue, bool) {
	for {
		keyVal, ok := iter.NextEntry()
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

func (iter *Iterator) NextEntry() (types.RowEntry, bool) {
	for {
		if iter.currentBlockIter.IsAbsent() {
			nextBlockIter, err := iter.nextBlockIter()
			if err != nil {
				return types.RowEntry{}, false
			}

			if nextBlockIter.IsPresent() {
				iter.currentBlockIter = nextBlockIter
			} else {
				return types.RowEntry{}, false
			}
		}

		currentBlockIter, _ := iter.currentBlockIter.Get()
		kv, ok := currentBlockIter.NextEntry()
		if !ok {
			if warn := currentBlockIter.Warnings(); warn != nil {
				iter.warn.Merge(warn)
			}
			// We have exhausted the current block, but not necessarily the entire SST,
			// so we fall back to the top to check if we have more blocks to read.
			iter.currentBlockIter = mo.None[*block.Iterator]()
			continue
		}

		return kv, true
	}
}

// SpawnFetches - Each SST has multiple blocks, this method will create goroutines to fetch blocks within a range
// Range{blocksStart, blocksEnd} for a given SST from object storage
// TODO(thrawn01): This is called from compaction, we should instead call it from the constructor and it should be
//  made private to this struct
func (iter *Iterator) SpawnFetches() {

	numBlocks := iter.indexData.BlockMetaLength()
	table := iter.table.Clone()
	// TODO(thrawn01): I don't believe we want to clone the store here. this
	//  this invalidates the cache and the mutex of the store, which likely
	//  will cause a race in TableStore.
	tableStore := iter.tableStore
	index := iter.indexData.Clone()
	var wg sync.WaitGroup

	for len(iter.fetchTasks) < int(iter.maxFetchTasks) && int(iter.nextBlockIdxToFetch) < numBlocks {
		numBlocksToFetch := math.Min(
			float64(iter.numBlocksToFetch),
			float64(numBlocks-int(iter.nextBlockIdxToFetch)),
		)
		blocksStart := iter.nextBlockIdxToFetch
		blocksEnd := iter.nextBlockIdxToFetch + uint64(numBlocksToFetch)

		blocksCh := make(chan mo.Option[[]block.Block], 1)
		iter.fetchTasks <- blocksCh

		blocksRange := common.Range{Start: blocksStart, End: blocksEnd}

		wg.Add(1)
		go func() {
			blocks, err := tableStore.ReadBlocksUsingIndex(table, blocksRange, index)
			if err != nil {
				// TODO(thrawn01): handle error
				blocksCh <- mo.None[[]block.Block]()
			} else {
				blocksCh <- mo.Some(blocks)
			}
			wg.Done()
		}()
		wg.Wait()

		iter.nextBlockIdxToFetch = blocksEnd
	}
}

func (iter *Iterator) nextBlockIter() (mo.Option[*block.Iterator], error) {
	for {
		iter.SpawnFetches()
		// TODO(thrawn01): This is a race, we should not expect an empty channel to indicate there are no more
		//  items to process.
		if len(iter.fetchTasks) == 0 {
			common.AssertTrue(int(iter.nextBlockIdxToFetch) == iter.indexData.BlockMetaLength(), "")
			fmt.Printf("Iteration Stopped Due To Empty Task Channel\n")
			return mo.None[*block.Iterator](), nil
		}

		blocksCh := <-iter.fetchTasks
		blocks := <-blocksCh
		if blocks.IsPresent() {
			blks, _ := blocks.Get()
			if len(blks) == 0 {
				continue
			}

			b := &blks[0]
			fromKey, _ := iter.fromKey.Get()
			if iter.fromKey.IsPresent() {
				// TODO(thrawn01): Handle this error
				it, _ := block.NewIteratorAtKey(b, fromKey)
				return mo.Some(it), nil
			} else {
				return mo.Some(block.NewIterator(b)), nil
			}
		} else {
			// TODO(thrawn01): Return the actual error which occurred.
			return mo.None[*block.Iterator](), common.ErrReadBlocks
		}
	}
}

func (iter *Iterator) firstBlockWithDataIncludingOrAfterKey(index *Index, key []byte) uint64 {
	low := 0
	high := index.BlockMetaLength() - 1
	// if the key is less than all the blocks' first key, scan the whole sst
	foundBlockID := 0

loop:
	for low <= high {
		mid := low + (high-low)/2
		midBlockFirstKey := index.BlockMeta()[mid].FirstKey
		cmp := bytes.Compare(midBlockFirstKey, key)
		switch cmp {
		case -1:
			low = mid + 1
			foundBlockID = mid
		case 1:
			if mid > 0 {
				high = mid - 1
			} else {
				break loop
			}
		case 0:
			return uint64(mid)
		}
	}

	return uint64(foundBlockID)
}

// Warnings returns types.ErrWarn if there was a warning during iteration.
func (iter *Iterator) Warnings() *types.ErrWarn {
	return &iter.warn
}

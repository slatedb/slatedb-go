package slatedb

import (
	"bytes"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"

	"github.com/samber/mo"
	"sort"
)

// ------------------------------------------------
// SortedRun
// ------------------------------------------------

type SortedRun struct {
	id      uint32
	sstList []sstable.Handle
}

func (s *SortedRun) indexOfSSTWithKey(key []byte) mo.Option[int] {
	index := sort.Search(len(s.sstList), func(i int) bool {
		assert.True(len(s.sstList[i].Info.FirstKey) != 0, "sst must have first key")
		return bytes.Compare(s.sstList[i].Info.FirstKey, key) > 0
	})
	if index > 0 {
		return mo.Some(index - 1)
	}
	return mo.None[int]()
}

func (s *SortedRun) sstWithKey(key []byte) mo.Option[sstable.Handle] {
	index, ok := s.indexOfSSTWithKey(key).Get()
	if ok {
		return mo.Some(s.sstList[index])
	}
	return mo.None[sstable.Handle]()
}

func (s *SortedRun) clone() *SortedRun {
	sstList := make([]sstable.Handle, 0, len(s.sstList))
	for _, sst := range s.sstList {
		sstList = append(sstList, *sst.Clone())
	}
	return &SortedRun{
		id:      s.id,
		sstList: sstList,
	}
}

// ------------------------------------------------
// SortedRunIterator
// ------------------------------------------------

type SortedRunIterator struct {
	currentKVIter     mo.Option[*sstable.Iterator]
	sstListIter       *SSTListIterator
	tableStore        *TableStore
	numBlocksToFetch  uint64
	numBlocksToBuffer uint64
	warn              types.ErrWarn
}

func newSortedRunIterator(
	sortedRun SortedRun,
	tableStore *TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
) (*SortedRunIterator, error) {
	return newSortedRunIter(sortedRun.sstList, tableStore, maxFetchTasks, numBlocksToFetch, mo.None[[]byte]())
}

func newSortedRunIteratorFromKey(
	sortedRun SortedRun,
	key []byte,
	tableStore *TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
) (*SortedRunIterator, error) {
	sstList := sortedRun.sstList
	idx, ok := sortedRun.indexOfSSTWithKey(key).Get()
	if ok {
		sstList = sortedRun.sstList[idx:]
	}

	return newSortedRunIter(sstList, tableStore, maxFetchTasks, numBlocksToFetch, mo.Some(key))
}

func newSortedRunIter(
	sstList []sstable.Handle,
	tableStore *TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
	fromKey mo.Option[[]byte],
) (*SortedRunIterator, error) {

	sstListIter := newSSTListIterator(sstList)
	currentKVIter := mo.None[*sstable.Iterator]()
	sst, ok := sstListIter.Next()
	if ok {
		var iter *sstable.Iterator
		var err error
		if fromKey.IsPresent() {
			key, _ := fromKey.Get()
			iter, err = sstable.NewIteratorAtKey(&sst, key, tableStore, maxFetchTasks, numBlocksToFetch)
			if err != nil {
				return nil, err
			}
		} else {
			iter, err = sstable.NewIterator(&sst, tableStore, maxFetchTasks, numBlocksToFetch)
			if err != nil {
				return nil, err
			}
		}

		currentKVIter = mo.Some(iter)
	}

	return &SortedRunIterator{
		currentKVIter:     currentKVIter,
		sstListIter:       sstListIter,
		tableStore:        tableStore,
		numBlocksToFetch:  maxFetchTasks,
		numBlocksToBuffer: numBlocksToFetch,
	}, nil
}

func (iter *SortedRunIterator) Next() (types.KeyValue, bool) {
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

func (iter *SortedRunIterator) NextEntry() (types.RowEntry, bool) {
	for {
		if iter.currentKVIter.IsAbsent() {
			return types.RowEntry{}, false
		}

		kvIter, _ := iter.currentKVIter.Get()
		kv, ok := kvIter.NextEntry()
		if ok {
			return kv, true
		} else {
			if warn := kvIter.Warnings(); warn != nil {
				iter.warn.Merge(warn)
			}
		}

		sst, ok := iter.sstListIter.Next()
		if !ok {
			if warn := kvIter.Warnings(); warn != nil {
				iter.warn.Merge(warn)
			}
			return types.RowEntry{}, false
		}

		newKVIter, err := sstable.NewIterator(&sst, iter.tableStore, iter.numBlocksToFetch, iter.numBlocksToBuffer)
		if err != nil {
			iter.warn.Add("while creating SSTable iterator: %s", err.Error())
			return types.RowEntry{}, false
		}

		iter.currentKVIter = mo.Some(newKVIter)
	}
}

// Warnings returns types.ErrWarn if there was a warning during iteration.
func (iter *SortedRunIterator) Warnings() *types.ErrWarn {
	return &iter.warn
}

// ------------------------------------------------
// SSTListIterator
// ------------------------------------------------

type SSTListIterator struct {
	sstList []sstable.Handle
	current int
}

func newSSTListIterator(sstList []sstable.Handle) *SSTListIterator {
	return &SSTListIterator{sstList, 0}
}

func (iter *SSTListIterator) Next() (sstable.Handle, bool) {
	if iter.current >= len(iter.sstList) {
		return sstable.Handle{}, false
	}
	sst := iter.sstList[iter.current]
	iter.current++
	return sst, true
}

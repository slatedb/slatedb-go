package slatedb

import (
	"bytes"

	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"sort"
)

// ------------------------------------------------
// SortedRun
// ------------------------------------------------

type SortedRun struct {
	id      uint32
	sstList []SSTableHandle
}

func (s *SortedRun) indexOfSSTWithKey(key []byte) mo.Option[int] {
	index := sort.Search(len(s.sstList), func(i int) bool {
		firstKey, ok := s.sstList[i].info.FirstKey.Get()
		common.AssertTrue(ok, "sst must have first key")
		return bytes.Compare(firstKey, key) > 0
	})
	if index > 0 {
		return mo.Some(index - 1)
	}
	return mo.None[int]()
}

func (s *SortedRun) sstWithKey(key []byte) mo.Option[SSTableHandle] {
	index, ok := s.indexOfSSTWithKey(key).Get()
	if ok {
		return mo.Some(s.sstList[index])
	}
	return mo.None[SSTableHandle]()
}

func (s *SortedRun) clone() *SortedRun {
	sstList := make([]SSTableHandle, 0, len(s.sstList))
	for _, sst := range s.sstList {
		sstList = append(sstList, *sst.clone())
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
	currentKVIter     mo.Option[*SSTIterator]
	sstListIter       *SSTListIterator
	tableStore        *TableStore
	numBlocksToFetch  uint64
	numBlocksToBuffer uint64
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
	sstList []SSTableHandle,
	tableStore *TableStore,
	maxFetchTasks uint64,
	numBlocksToFetch uint64,
	fromKey mo.Option[[]byte],
) (*SortedRunIterator, error) {

	sstListIter := newSSTListIterator(sstList)
	currentKVIter := mo.None[*SSTIterator]()
	sst, ok := sstListIter.Next()
	if ok {
		var iter *SSTIterator
		var err error
		if fromKey.IsPresent() {
			key, _ := fromKey.Get()
			iter, err = newSSTIteratorFromKey(&sst, key, tableStore, maxFetchTasks, numBlocksToFetch)
			if err != nil {
				return nil, err
			}
		} else {
			iter, err = newSSTIterator(&sst, tableStore, maxFetchTasks, numBlocksToFetch)
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

func (iter *SortedRunIterator) Next() (common.KV, bool) {
	for {
		keyVal, ok := iter.NextEntry()
		if !ok {
			return common.KV{}, false
		}
		if keyVal.ValueDel.IsTombstone {
			continue
		}

		return common.KV{
			Key:   keyVal.Key,
			Value: keyVal.ValueDel.Value,
		}, true
	}
}

func (iter *SortedRunIterator) NextEntry() (common.KVDeletable, bool) {
	for {
		if iter.currentKVIter.IsAbsent() {
			return common.KVDeletable{}, false
		}

		kvIter, _ := iter.currentKVIter.Get()
		kv, ok := kvIter.NextEntry()
		if ok {
			return kv, true
		}

		sst, ok := iter.sstListIter.Next()
		if !ok {
			return common.KVDeletable{}, false
		}

		newKVIter, err := newSSTIterator(&sst, iter.tableStore, iter.numBlocksToFetch, iter.numBlocksToBuffer)
		if err != nil {
			return common.KVDeletable{}, false
		}

		iter.currentKVIter = mo.Some(newKVIter)
	}
}

// ------------------------------------------------
// SSTListIterator
// ------------------------------------------------

type SSTListIterator struct {
	sstList []SSTableHandle
	current int
}

func newSSTListIterator(sstList []SSTableHandle) *SSTListIterator {
	return &SSTListIterator{sstList, 0}
}

func (iter *SSTListIterator) Next() (SSTableHandle, bool) {
	if iter.current >= len(iter.sstList) {
		return SSTableHandle{}, false
	}
	sst := iter.sstList[iter.current]
	iter.current++
	return sst, true
}

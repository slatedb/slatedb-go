package slatedb

import (
	"bytes"
	"github.com/samber/mo"
)

// ------------------------------------------------
// SortedRun
// ------------------------------------------------

type SortedRun struct {
	id      uint32
	sstList []SSTableHandle
}

func (s *SortedRun) indexOfSSTWithKey(key []byte) mo.Option[int] {
	index := 0
	for i, sst := range s.sstList {
		firstKey := sst.info.borrow().FirstKeyBytes()
		if bytes.Compare(firstKey, key) > 0 {
			index = i
			break
		}
	}
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

// ------------------------------------------------
// SortedRunIterator
// ------------------------------------------------

type SortedRunIterator struct {
	currentIter       mo.Option[SSTIterator]
	tableStore        *TableStore
	numBlocksToFetch  uint64
	numBlocksToBuffer uint64
}

func newSortedRunIterator(run SortedRun, store *TableStore, maxFetchTasks uint64, numBlocksToFetch uint64) *SortedRunIterator {
	return nil
}

func newSortedRunIteratorFromKey(run SortedRun, store *TableStore) {

}

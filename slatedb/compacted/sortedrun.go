package compacted

import (
	"bytes"
	"context"
	"sort"

	"github.com/samber/mo"

	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
)

// ------------------------------------------------
// SortedRun
// ------------------------------------------------

type SortedRun struct {
	ID      uint32
	SSTList []sstable.Handle
}

func (s *SortedRun) indexOfSSTWithKey(key []byte) mo.Option[int] {
	index := sort.Search(len(s.SSTList), func(i int) bool {
		assert.True(len(s.SSTList[i].Info.FirstKey) != 0, "sst must have first key")
		return bytes.Compare(s.SSTList[i].Info.FirstKey, key) > 0
	})
	if index > 0 {
		return mo.Some(index - 1)
	}
	return mo.None[int]()
}

func (s *SortedRun) SstWithKey(key []byte) mo.Option[sstable.Handle] {
	index, ok := s.indexOfSSTWithKey(key).Get()
	if ok {
		return mo.Some(s.SSTList[index])
	}
	return mo.None[sstable.Handle]()
}

func (s *SortedRun) Clone() *SortedRun {
	sstList := make([]sstable.Handle, 0, len(s.SSTList))
	for _, sst := range s.SSTList {
		sstList = append(sstList, *sst.Clone())
	}
	return &SortedRun{
		ID:      s.ID,
		SSTList: sstList,
	}
}

// ------------------------------------------------
// SortedRunIterator
// ------------------------------------------------

type SortedRunIterator struct {
	currentKVIter mo.Option[*sstable.Iterator]
	sstListIter   *SSTListIterator
	tableStore    sstable.TableStore
	warn          types.ErrWarn
}

func NewSortedRunIterator(ctx context.Context, sr SortedRun, store sstable.TableStore) (*SortedRunIterator, error) {
	return newSortedRunIter(ctx, sr.SSTList, store, mo.None[[]byte]())
}

func NewSortedRunIteratorFromKey(ctx context.Context, sr SortedRun, key []byte, store sstable.TableStore) (*SortedRunIterator, error) {
	sstList := sr.SSTList
	idx, ok := sr.indexOfSSTWithKey(key).Get()
	if ok {
		sstList = sr.SSTList[idx:]
	}

	return newSortedRunIter(ctx, sstList, store, mo.Some(key))
}

func newSortedRunIter(ctx context.Context,
	sstList []sstable.Handle,
	store sstable.TableStore,
	fromKey mo.Option[[]byte]) (*SortedRunIterator, error) {

	sstListIter := newSSTListIterator(sstList)
	currentKVIter := mo.None[*sstable.Iterator]()
	sst, ok := sstListIter.Next()
	if ok {
		var iter *sstable.Iterator
		var err error
		if fromKey.IsPresent() {
			key, _ := fromKey.Get()
			iter, err = sstable.NewIteratorAtKey(ctx, &sst, key, store)
			if err != nil {
				return nil, err
			}
		} else {
			iter, err = sstable.NewIterator(ctx, &sst, store)
			if err != nil {
				return nil, err
			}
		}

		currentKVIter = mo.Some(iter)
	}

	return &SortedRunIterator{
		currentKVIter: currentKVIter,
		sstListIter:   sstListIter,
		tableStore:    store,
	}, nil
}

func (iter *SortedRunIterator) NextEntry(ctx context.Context) (types.RowEntry, bool) {
	for {
		if iter.currentKVIter.IsAbsent() {
			return types.RowEntry{}, false
		}

		kvIter, _ := iter.currentKVIter.Get()
		kv, ok := kvIter.NextEntry(ctx)
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

		newKVIter, err := sstable.NewIterator(ctx, &sst, iter.tableStore)
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

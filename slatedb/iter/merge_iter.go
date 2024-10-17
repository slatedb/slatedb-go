package iter

import (
	"bytes"
	"cmp"
	"container/heap"

	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"go.uber.org/zap"
)

// ------------------------------------------------
// MergeIterator
// ------------------------------------------------

// This is to merge KVIterators. Each KVIterator has a list of KV pairs. When we call Next() on the MergeIterator,
// it should give us the least Key among all the keys in the merged list

// MergeIterator -
// This struct will keep one KVIterator in MergeIterator.current (this will have the least Key among all the KVIterators)
// and the remaining KVIterators in the MergeIterator.iterators heap.
// When we want to advance the iterator we read MergeIterator.current, then we push the current KVIterator to heap and
// let the heap decide which KVIterator has the next least Key (by calling heap.Pop()) and should replace MergeIterator.current.
type MergeIterator struct {
	current   mo.Option[MergeIteratorHeapEntry]
	iterators *MergeIteratorHeap
}

func NewMergeIterator(iterators []KVIterator) *MergeIterator {
	h := &MergeIteratorHeap{}
	heap.Init(h)
	index := uint32(0)
	for len(iterators) > 0 {
		iterator := iterators[0]
		iterators = iterators[1:]
		entry, err := iterator.NextEntry()
		if err != nil {
			return nil
		}
		if entry.IsPresent() {
			kv, _ := entry.Get()
			heapEntry := MergeIteratorHeapEntry{
				nextKV:   kv,
				index:    index,
				iterator: iterator,
			}
			heap.Push(h, heapEntry)
		}
		index += 1
	}
	current := heap.Pop(h).(MergeIteratorHeapEntry)
	return &MergeIterator{
		current:   mo.Some(current),
		iterators: h,
	}
}

func (m *MergeIterator) advance() (mo.Option[common.KVDeletable], error) {
	if m.current.IsAbsent() {
		return mo.None[common.KVDeletable](), nil
	}

	iteratorState, _ := m.current.Get()
	currentKV := iteratorState.nextKV
	entry, err := iteratorState.iterator.NextEntry()
	if err != nil {
		logger.Error("unable to get key value options", zap.Error(err))
		return mo.None[common.KVDeletable](), err
	}

	heap.Init(m.iterators)
	if entry.IsPresent() {
		kv, _ := entry.Get()
		iteratorState.nextKV = kv
		heap.Push(m.iterators, iteratorState)
	}
	if m.iterators.Len() == 0 {
		m.current = mo.None[MergeIteratorHeapEntry]()
	} else {
		current := heap.Pop(m.iterators).(MergeIteratorHeapEntry)
		m.current = mo.Some(current)
	}
	return mo.Some(currentKV), nil
}

func (m *MergeIterator) Next() (mo.Option[common.KV], error) {
	for {
		entry, err := m.NextEntry()
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

func (m *MergeIterator) NextEntry() (mo.Option[common.KVDeletable], error) {
	kvOption, err := m.advance()
	if err != nil {
		return mo.None[common.KVDeletable](), err
	}

	if kvOption.IsAbsent() {
		return mo.None[common.KVDeletable](), nil
	}

	kv, _ := kvOption.Get()
	// this loop is to iterate over duplicate keys. we break once we get a key that is not a duplicate
	for m.current.IsPresent() {
		nextEntry, _ := m.current.Get()
		if bytes.Compare(nextEntry.nextKV.Key, kv.Key) != 0 {
			break
		}

		_, err := m.advance()
		if err != nil {
			return mo.None[common.KVDeletable](), err
		}
	}
	return mo.Some(kv), nil
}

// ------------------------------------------------
// TwoMergeIterator
// ------------------------------------------------

type iterKV struct {
	iter KVIterator
	kv   mo.Option[common.KVDeletable]
}

type TwoMergeIterator struct {
	iterator1 iterKV
	iterator2 iterKV
}

func NewTwoMergeIterator(iter1 KVIterator, iter2 KVIterator) (*TwoMergeIterator, error) {
	next1, err := iter1.NextEntry()
	if err != nil {
		logger.Error("unable to get next entry for iteration 1", zap.Error(err))
		return nil, err
	}

	next2, err := iter2.NextEntry()
	if err != nil {
		logger.Error("unable to get next entry for iteration 2", zap.Error(err))
		return nil, err
	}

	return &TwoMergeIterator{
		iterator1: iterKV{iter1, next1},
		iterator2: iterKV{iter2, next2},
	}, nil
}

func (t *TwoMergeIterator) advance1() (mo.Option[common.KVDeletable], error) {
	if t.iterator1.kv.IsAbsent() {
		return mo.None[common.KVDeletable](), nil
	}

	oldKV := t.iterator1.kv
	nextKV, err := t.iterator1.iter.NextEntry()
	if err != nil {
		return mo.None[common.KVDeletable](), nil
	}

	t.iterator1.kv = nextKV
	return oldKV, nil
}

func (t *TwoMergeIterator) advance2() (mo.Option[common.KVDeletable], error) {
	if t.iterator2.kv.IsAbsent() {
		return mo.None[common.KVDeletable](), nil
	}

	oldKV := t.iterator2.kv
	nextKV, err := t.iterator2.iter.NextEntry()
	if err != nil {
		return mo.None[common.KVDeletable](), nil
	}

	t.iterator2.kv = nextKV
	return oldKV, nil
}

func (t *TwoMergeIterator) Next() (mo.Option[common.KV], error) {
	for {
		entry, err := t.NextEntry()
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

func (t *TwoMergeIterator) NextEntry() (mo.Option[common.KVDeletable], error) {
	if t.iterator1.kv.IsAbsent() {
		return t.advance2()
	}
	if t.iterator2.kv.IsAbsent() {
		return t.advance1()
	}

	next1, _ := t.iterator1.kv.Get()
	next2, _ := t.iterator2.kv.Get()
	cmpValue := bytes.Compare(next1.Key, next2.Key)
	if cmpValue <= 0 {
		if cmpValue == 0 {
			_, err := t.advance2()
			if err != nil {
				return mo.None[common.KVDeletable](), err
			}
		}
		return t.advance1()
	}
	return t.advance2()
}

// ------------------------------------------------
// MergeIteratorHeap
// ------------------------------------------------

// MergeIteratorHeap - Each MergeIteratorHeapEntry in the heap holds a KVIterator and when we pop from the heap,
// it returns the MergeIteratorHeapEntry with the least Key in MergeIteratorHeapEntry.nextKV.
// So this heap helps in returning the keys in ascending order from the merged KVIterators
type MergeIteratorHeap []MergeIteratorHeapEntry

// The following 5 methods Len(), Less(), Swap(), Push() and Pop() are required to use the heap functionality
// https://pkg.go.dev/container/heap
// The keys can be pushed in any order to the heap. It will return the keys in ascending order when we call heap.Pop()

func (h MergeIteratorHeap) Len() int { return len(h) }

func (h MergeIteratorHeap) Less(i, j int) bool {
	return h[i].Compare(h[j]) < 0
}

func (h MergeIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MergeIteratorHeap) Push(x any) {
	*h = append(*h, x.(MergeIteratorHeapEntry))
}

func (h *MergeIteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MergeIteratorHeapEntry struct {
	nextKV   common.KVDeletable
	index    uint32
	iterator KVIterator
}

func (e *MergeIteratorHeapEntry) Compare(other MergeIteratorHeapEntry) int {
	cmpValue := bytes.Compare(e.nextKV.Key, other.nextKV.Key)
	if cmpValue == 0 {
		return cmp.Compare(e.index, other.index)
	}
	return cmpValue
}

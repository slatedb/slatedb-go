package iter

import (
	"bytes"
	"cmp"
	"container/heap"
	"github.com/slatedb/slatedb-go/internal/types"
)

type MergeSort struct {
	iterators []KVIterator
	heap      minHeap
	lastKey   []byte
	warn      types.ErrWarn
}

// NewMergeSort performs a merge sort on values of each iterator. Each iterator provided
// is assumed to also be a sorted iterator. As such, the MergeSort will efficiently sort
// as iteration continues. Additionally, if duplicate keys are encountered during iteration
// only the first key of the duplicates will be considered, any duplicate keys from future
// iterations are discarded. Higher precedence for duplicate keys is given to keys that
// come from iterators ordered first in the list of provided iterators.
//
// Precedence example:
// Given an iterator in the list at index 0 which has key 'a'
// and an iterator in the list at index 1 which also has key 'a'
// the key value from the iterator at index 0 will be used.
func NewMergeSort(iterators ...KVIterator) *MergeSort {
	ms := &MergeSort{
		iterators: iterators,
		heap:      make(minHeap, 0, len(iterators)),
	}

	// Initialize the heap with the first element from each iterator
	for i, iter := range iterators {
		if kv, ok := iter.NextEntry(); ok {
			heap.Push(&ms.heap, heapItem{kv: kv, index: i})
		}

		if warn := iter.Warnings(); warn != nil {
			ms.warn.Merge(warn)
		}
	}
	heap.Init(&ms.heap)

	return ms
}

func (m *MergeSort) Next() (types.KeyValue, bool) {
	for {
		entry, ok := m.NextEntry()
		if !ok {
			return types.KeyValue{}, false
		}
		if !entry.Value.IsTombstone() {
			return types.KeyValue{Key: entry.Key, Value: entry.Value.Value}, true
		}
	}
}

// NextEntry Returns the next entry in the iterator, which may be a key-value pair or
// a tombstone of a deleted key-value pair.
func (m *MergeSort) NextEntry() (types.RowEntry, bool) {
	for m.heap.Len() > 0 {
		item := heap.Pop(&m.heap).(heapItem)
		result := item.kv

		// Push the next item from the same iterator
		if nextKV, ok := m.iterators[item.index].NextEntry(); ok {
			heap.Push(&m.heap, heapItem{kv: nextKV, index: item.index})
		} else {
			m.warn.Merge(m.iterators[item.index].Warnings())
		}

		// Check if this key is different from the last one
		if !bytes.Equal(result.Key, m.lastKey) {
			m.lastKey = result.Key
			return result, true
		}

		// If it's the same key, continue to the next item
	}
	return types.RowEntry{}, false
}

// Warnings returns types.ErrWarn if there was a warning during iteration.
func (m *MergeSort) Warnings() *types.ErrWarn {
	return &m.warn
}

// heapItem is used in the Sorted Heap
type heapItem struct {
	kv    types.RowEntry
	index int
}

type minHeap []heapItem

func (e heapItem) Compare(other heapItem) int {
	cmpValue := bytes.Compare(e.kv.Key, other.kv.Key)
	if cmpValue == 0 {
		return cmp.Compare(e.index, other.index)
	}
	return cmpValue
}

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].Compare(h[j]) < 0 }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

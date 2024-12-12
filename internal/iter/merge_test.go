package iter_test

import (
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/iter"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMergeUniqueIteratorPrecedence(t *testing.T) {
	iters := make([]iter.KVIterator, 0)
	iters = append(iters, iter.NewEntryIterator().
		Add([]byte("aaaa"), []byte("1111")).
		Add([]byte("cccc"), []byte("use this one c")),
	)
	iters = append(iters, iter.NewEntryIterator().
		Add([]byte("cccc"), []byte("badc1")).
		Add([]byte("xxxx"), []byte("use this one x")),
	)
	iters = append(iters, iter.NewEntryIterator().
		Add([]byte("bbbb"), []byte("2222")).
		Add([]byte("cccc"), []byte("badc2")).
		Add([]byte("xxxx"), []byte("badx1")),
	)

	mergeIter := iter.NewMergeSort(iters...)
	assert2.NextEntry(t, mergeIter, []byte("aaaa"), []byte("1111"))
	assert2.NextEntry(t, mergeIter, []byte("bbbb"), []byte("2222"))
	assert2.NextEntry(t, mergeIter, []byte("cccc"), []byte("use this one c"))
	assert2.NextEntry(t, mergeIter, []byte("xxxx"), []byte("use this one x"))

	_, ok := mergeIter.Next()
	assert.False(t, ok, "Expected no more entries")
}

func TestMergeUnique(t *testing.T) {
	iters := make([]iter.KVIterator, 0)
	iters = append(iters, iter.NewEntryIterator().
		Add([]byte("aaaa"), []byte("1111")).
		Add([]byte("cccc"), []byte("3333")).
		Add([]byte("zzzz"), []byte("26262626")),
	)
	iters = append(iters, iter.NewEntryIterator().
		Add([]byte("bbbb"), []byte("2222")).
		Add([]byte("xxxx"), []byte("24242424")).
		Add([]byte("yyyy"), []byte("25252525")),
	)
	iters = append(iters, iter.NewEntryIterator().
		Add([]byte("dddd"), []byte("4444")).
		Add([]byte("eeee"), []byte("5555")).
		Add([]byte("gggg"), []byte("7777")),
	)

	mergeIter := iter.NewMergeSort(iters...)
	assert2.NextEntry(t, mergeIter, []byte("aaaa"), []byte("1111"))
	assert2.NextEntry(t, mergeIter, []byte("bbbb"), []byte("2222"))
	assert2.NextEntry(t, mergeIter, []byte("cccc"), []byte("3333"))
	assert2.NextEntry(t, mergeIter, []byte("dddd"), []byte("4444"))
	assert2.NextEntry(t, mergeIter, []byte("eeee"), []byte("5555"))
	assert2.NextEntry(t, mergeIter, []byte("gggg"), []byte("7777"))
	assert2.NextEntry(t, mergeIter, []byte("xxxx"), []byte("24242424"))
	assert2.NextEntry(t, mergeIter, []byte("yyyy"), []byte("25252525"))
	assert2.NextEntry(t, mergeIter, []byte("zzzz"), []byte("26262626"))

	_, ok := mergeIter.Next()
	assert.False(t, ok, "Expected no more entries")
}

func TestMergeSortTwoIterators(t *testing.T) {
	iter1 := iter.NewEntryIterator().
		Add([]byte("aaaa"), []byte("1111")).
		Add([]byte("cccc"), []byte("3333")).
		Add([]byte("zzzz"), []byte("26262626"))

	iter2 := iter.NewEntryIterator().
		Add([]byte("bbbb"), []byte("2222")).
		Add([]byte("xxxx"), []byte("24242424")).
		Add([]byte("yyyy"), []byte("25252525"))

	mergeIter := iter.NewMergeSort(iter1, iter2)
	assert2.NextEntry(t, mergeIter, []byte("aaaa"), []byte("1111"))
	assert2.NextEntry(t, mergeIter, []byte("bbbb"), []byte("2222"))
	assert2.NextEntry(t, mergeIter, []byte("cccc"), []byte("3333"))
	assert2.NextEntry(t, mergeIter, []byte("xxxx"), []byte("24242424"))
	assert2.NextEntry(t, mergeIter, []byte("yyyy"), []byte("25252525"))
	assert2.NextEntry(t, mergeIter, []byte("zzzz"), []byte("26262626"))

	_, ok := mergeIter.Next()
	assert.False(t, ok, "Expected no more entries")
}

func TestMergeSortTwoIteratorsPrecedence(t *testing.T) {
	iter1 := iter.NewEntryIterator().
		Add([]byte("aaaa"), []byte("1111")).
		Add([]byte("cccc"), []byte("use this one c"))

	iter2 := iter.NewEntryIterator().
		Add([]byte("cccc"), []byte("badc")).
		Add([]byte("xxxx"), []byte("24242424"))

	mergeIter := iter.NewMergeSort(iter1, iter2)
	assert2.NextEntry(t, mergeIter, []byte("aaaa"), []byte("1111"))
	assert2.NextEntry(t, mergeIter, []byte("cccc"), []byte("use this one c"))
	assert2.NextEntry(t, mergeIter, []byte("xxxx"), []byte("24242424"))

	_, ok := mergeIter.Next()
	assert.False(t, ok, "Expected no more entries")
}

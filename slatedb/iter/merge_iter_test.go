package iter

import (
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMergeIteratorShouldIncludeEntriesInOrder(t *testing.T) {
	iters := make([]KVIterator, 0)
	iters = append(iters, newTestIterator().
		withEntry([]byte("aaaa"), []byte("1111")).
		withEntry([]byte("cccc"), []byte("3333")).
		withEntry([]byte("zzzz"), []byte("26262626")),
	)
	iters = append(iters, newTestIterator().
		withEntry([]byte("bbbb"), []byte("2222")).
		withEntry([]byte("xxxx"), []byte("24242424")).
		withEntry([]byte("yyyy"), []byte("25252525")),
	)
	iters = append(iters, newTestIterator().
		withEntry([]byte("dddd"), []byte("4444")).
		withEntry([]byte("eeee"), []byte("5555")).
		withEntry([]byte("gggg"), []byte("7777")),
	)

	mergeIter := newMergeIterator(iters)
	AssertIterNextEntry(t, mergeIter, []byte("aaaa"), []byte("1111"))
	AssertIterNextEntry(t, mergeIter, []byte("bbbb"), []byte("2222"))
	AssertIterNextEntry(t, mergeIter, []byte("cccc"), []byte("3333"))
	AssertIterNextEntry(t, mergeIter, []byte("dddd"), []byte("4444"))
	AssertIterNextEntry(t, mergeIter, []byte("eeee"), []byte("5555"))
	AssertIterNextEntry(t, mergeIter, []byte("gggg"), []byte("7777"))
	AssertIterNextEntry(t, mergeIter, []byte("xxxx"), []byte("24242424"))
	AssertIterNextEntry(t, mergeIter, []byte("yyyy"), []byte("25252525"))
	AssertIterNextEntry(t, mergeIter, []byte("zzzz"), []byte("26262626"))
}

func TestMergeIteratorShouldWriteOneEntryWithGivenKey(t *testing.T) {
	iters := make([]KVIterator, 0)
	iters = append(iters, newTestIterator().
		withEntry([]byte("aaaa"), []byte("1111")).
		withEntry([]byte("cccc"), []byte("use this one c")),
	)
	iters = append(iters, newTestIterator().
		withEntry([]byte("cccc"), []byte("badc1")).
		withEntry([]byte("xxxx"), []byte("use this one x")),
	)
	iters = append(iters, newTestIterator().
		withEntry([]byte("bbbb"), []byte("2222")).
		withEntry([]byte("cccc"), []byte("badc2")).
		withEntry([]byte("xxxx"), []byte("badx1")),
	)

	mergeIter := newMergeIterator(iters)
	AssertIterNextEntry(t, mergeIter, []byte("aaaa"), []byte("1111"))
	AssertIterNextEntry(t, mergeIter, []byte("bbbb"), []byte("2222"))
	AssertIterNextEntry(t, mergeIter, []byte("cccc"), []byte("use this one c"))
	AssertIterNextEntry(t, mergeIter, []byte("xxxx"), []byte("use this one x"))
}

func TestTwoIteratorShouldIncludeEntriesInOrder(t *testing.T) {
	iter1 := newTestIterator().
		withEntry([]byte("aaaa"), []byte("1111")).
		withEntry([]byte("cccc"), []byte("3333")).
		withEntry([]byte("zzzz"), []byte("26262626"))

	iter2 := newTestIterator().
		withEntry([]byte("bbbb"), []byte("2222")).
		withEntry([]byte("xxxx"), []byte("24242424")).
		withEntry([]byte("yyyy"), []byte("25252525"))

	mergeIter, err := newTwoMergeIterator(iter1, iter2)
	assert.NoError(t, err)
	AssertIterNextEntry(t, mergeIter, []byte("aaaa"), []byte("1111"))
	AssertIterNextEntry(t, mergeIter, []byte("bbbb"), []byte("2222"))
	AssertIterNextEntry(t, mergeIter, []byte("cccc"), []byte("3333"))
	AssertIterNextEntry(t, mergeIter, []byte("xxxx"), []byte("24242424"))
	AssertIterNextEntry(t, mergeIter, []byte("yyyy"), []byte("25252525"))
	AssertIterNextEntry(t, mergeIter, []byte("zzzz"), []byte("26262626"))
}

func TestTwoIteratorShouldWriteOneEntryWithGivenKey(t *testing.T) {
	iter1 := newTestIterator().
		withEntry([]byte("aaaa"), []byte("1111")).
		withEntry([]byte("cccc"), []byte("use this one c"))

	iter2 := newTestIterator().
		withEntry([]byte("cccc"), []byte("badc")).
		withEntry([]byte("xxxx"), []byte("24242424"))

	mergeIter, err := newTwoMergeIterator(iter1, iter2)
	assert.NoError(t, err)
	AssertIterNextEntry(t, mergeIter, []byte("aaaa"), []byte("1111"))
	AssertIterNextEntry(t, mergeIter, []byte("cccc"), []byte("use this one c"))
	AssertIterNextEntry(t, mergeIter, []byte("xxxx"), []byte("24242424"))
}

type result struct {
	kv  common.KVDeletable
	err error
}

type TestIterator struct {
	entries []result
}

func newTestIterator() *TestIterator {
	return &TestIterator{}
}

func (it *TestIterator) withEntry(key []byte, val []byte) *TestIterator {
	it.entries = append(it.entries, result{
		kv: common.KVDeletable{
			Key:      key,
			ValueDel: common.ValueDeletable{Value: val},
		},
	})
	return it
}

func (it *TestIterator) Next() (mo.Option[common.KV], error) {
	for {
		entry, err := it.NextEntry()
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

func (it *TestIterator) NextEntry() (mo.Option[common.KVDeletable], error) {
	if len(it.entries) == 0 {
		return mo.None[common.KVDeletable](), nil
	}
	result := it.entries[0]
	it.entries = it.entries[1:]
	if result.err != nil {
		return mo.None[common.KVDeletable](), result.err
	}
	return mo.Some(result.kv), nil
}

package table

import (
	"github.com/huandu/skiplist"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"math"
	"sync"
	"sync/atomic"
)

// ------------------------------------------------
// KVTable
// ------------------------------------------------

type KVTable struct {
	isDurableLock sync.Mutex
	isDurableCh   chan chan bool

	// skl skipList stores key ([]byte), value (ValueDeletable) pairs
	skl  *skiplist.SkipList
	size atomic.Int64
}

func newKVTable() *KVTable {
	return &KVTable{
		skl:         skiplist.New(skiplist.Bytes),
		isDurableCh: make(chan chan bool, math.MaxUint8),
	}
}

func (t *KVTable) get(key []byte) mo.Option[common.ValueDeletable] {
	elem := t.skl.Get(key)
	if elem == nil {
		return mo.None[common.ValueDeletable]()
	}
	return mo.Some(elem.Value.(common.ValueDeletable))
}

func (t *KVTable) put(key []byte, value []byte) int64 {
	t.maybeSubtractOldValFromSize(key)
	valueDel := common.ValueDeletable{
		Value:       value,
		IsTombstone: false,
	}
	t.skl.Set(key, valueDel)
	newSize := int64(len(key)) + valueDel.Size()
	t.size.Add(newSize)
	return newSize
}

func (t *KVTable) delete(key []byte) {
	t.maybeSubtractOldValFromSize(key)
	valueDel := common.ValueDeletable{
		Value:       nil,
		IsTombstone: true,
	}
	t.skl.Set(key, valueDel)
	newSize := int64(len(key)) + valueDel.Size()
	t.size.Add(newSize)
}

func (t *KVTable) iter() *KVTableIterator {
	return newKVTableIterator(t.skl.Front())
}

func (t *KVTable) rangeFrom(start []byte) *KVTableIterator {
	elem := t.skl.Find(start)
	return newKVTableIterator(elem)
}

// if key is present then subtract the size of key+value from KVTable.size
func (t *KVTable) maybeSubtractOldValFromSize(key []byte) {
	oldValue := t.get(key)
	if oldValue.IsPresent() {
		oldSize := int64(len(key)) + oldValue.MustGet().Size()
		t.size.Add(-oldSize)
	}
}

func (t *KVTable) AwaitWALFlush() <-chan bool {
	t.isDurableLock.Lock()
	defer t.isDurableLock.Unlock()
	done := make(chan bool, 1)
	t.isDurableCh <- done
	return done
}

func (t *KVTable) NotifyWALFlushed() {
	t.isDurableLock.Lock()
	defer t.isDurableLock.Unlock()
	if len(t.isDurableCh) == 0 {
		return
	}
	for i := 0; i < len(t.isDurableCh); i++ {
		done := <-t.isDurableCh
		if len(done) == 0 {
			done <- true
			close(done)
		}
	}
	t.isDurableCh = make(chan chan bool, math.MaxUint8)
}

func (t *KVTable) clone() *KVTable {
	skl := skiplist.New(skiplist.Bytes)
	current := t.skl.Front()
	for current != nil {
		key := current.Key().([]byte)
		val := current.Value.(common.ValueDeletable)
		skl.Set(key, val)
		current = current.Next()
	}
	return &KVTable{
		isDurableCh: t.isDurableCh,
		skl:         skl,
	}
}

// ------------------------------------------------
// KVTableIterator
// ------------------------------------------------

type KVTableIterator struct {
	element *skiplist.Element
}

func newKVTableIterator(element *skiplist.Element) *KVTableIterator {
	return &KVTableIterator{
		element: element,
	}
}

func (iter *KVTableIterator) Next() (mo.Option[common.KV], error) {
	for {
		entry, err := iter.NextEntry()
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

func (iter *KVTableIterator) NextEntry() (mo.Option[common.KVDeletable], error) {
	elem := iter.element
	if elem == nil {
		return mo.None[common.KVDeletable](), nil
	}

	iter.element = iter.element.Next()

	return mo.Some(common.KVDeletable{
		Key:      elem.Key().([]byte),
		ValueDel: elem.Value.(common.ValueDeletable),
	}), nil
}

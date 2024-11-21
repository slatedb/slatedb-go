package table

import (
	"github.com/huandu/skiplist"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
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
	skl *skiplist.SkipList

	// size of KVTable changes when we put/delete a key
	size atomic.Int64
}

func newKVTable() *KVTable {
	return &KVTable{
		skl:         skiplist.New(skiplist.Bytes),
		isDurableCh: make(chan chan bool, 1),
	}
}

func (t *KVTable) get(key []byte) mo.Option[common.ValueDeletable] {
	elem := t.skl.Get(key)
	if elem == nil {
		return mo.None[common.ValueDeletable]()
	}

	val := elem.Value.([]byte)
	return mo.Some(common.ValueDelFromBytes(val))
}

func (t *KVTable) put(key []byte, value []byte) int64 {
	oldSize := t.existingKVSize(key)
	valueDel := common.ValueDeletable{
		Value:       value,
		IsTombstone: false,
	}
	valueBytes := valueDel.ToBytes()
	t.skl.Set(key, valueBytes)

	newSize := int64(len(key) + len(valueBytes))
	t.size.Add(newSize - oldSize)
	return newSize
}

func (t *KVTable) delete(key []byte) {
	oldSize := t.existingKVSize(key)
	valueDel := common.ValueDeletable{IsTombstone: true}
	valueBytes := valueDel.ToBytes()
	t.skl.Set(key, valueBytes)

	newSize := int64(len(key) + len(valueBytes))
	t.size.Add(newSize - oldSize)
}

func (t *KVTable) iter() *KVTableIterator {
	return newKVTableIterator(t.skl.Front())
}

func (t *KVTable) rangeFrom(start []byte) *KVTableIterator {
	elem := t.skl.Find(start)
	return newKVTableIterator(elem)
}

func (t *KVTable) existingKVSize(key []byte) int64 {
	value := t.get(key)
	if value.IsPresent() {
		return int64(len(key) + len(value.MustGet().ToBytes()))
	}
	return 0
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
	t.isDurableCh = make(chan chan bool, 1)
}

func (t *KVTable) toBytes() []byte {
	current := t.skl.Front()
	resBytes := make([]byte, 0)
	for current != nil {
		elem := current.Element()
		resBytes = append(resBytes, elem.Key().([]byte)...)
		resBytes = append(resBytes, elem.Value.([]byte)...)
		current = current.Next()
	}
	return resBytes
}

func (t *KVTable) clone() *KVTable {
	skl := skiplist.New(skiplist.Bytes)
	current := t.skl.Front()
	for current != nil {
		key := current.Key().([]byte)
		val := current.Value.([]byte)
		skl.Set(key, val)
		current = current.Next()
	}

	return &KVTable{
		isDurableCh: make(chan chan bool, 1),
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

	valueBytes := elem.Value.([]byte)
	return mo.Some(common.KVDeletable{
		Key:      elem.Key().([]byte),
		ValueDel: common.ValueDelFromBytes(valueBytes),
	}), nil
}

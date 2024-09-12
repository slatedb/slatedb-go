package internal

import (
	"bytes"
	"github.com/huandu/skiplist"
	"github.com/samber/mo"
	"sync"
	"sync/atomic"
)

type Bytes []byte

func (b Bytes) Compare(lhs, rhs interface{}) int {
	return bytes.Compare(lhs.([]byte), rhs.([]byte))
}

func (b Bytes) CalcScore(key interface{}) float64 {
	return 0
}

// ------------------------------------------------
// KVTable
// ------------------------------------------------

type KVTable struct {
	mu sync.RWMutex

	// skl skipList stores key ([]byte), value (ValueDeletable) pairs
	skl           *skiplist.SkipList
	durableNotify chan string
}

func newKVTable() *KVTable {
	return &KVTable{
		skl: skiplist.New(Bytes{}),
	}
}

func (k *KVTable) get(key []byte) mo.Option[ValueDeletable] {
	k.mu.RLock()
	defer k.mu.RUnlock()

	elem := k.skl.Get(key)
	if elem == nil {
		return mo.None[ValueDeletable]()
	}
	return mo.Some[ValueDeletable](elem.Value.(ValueDeletable))
}

func (k *KVTable) put(key []byte, value []byte) int64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	valueDel := ValueDeletable{
		value:       value,
		isTombstone: false,
	}
	k.skl.Set(key, valueDel)
	return int64(len(key)) + valueDel.size()
}

func (k *KVTable) delete(key []byte) int64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	elem := k.skl.Get(key)
	if elem == nil {
		return 0
	}

	valueDel := ValueDeletable{
		value:       nil,
		isTombstone: true,
	}
	k.skl.Set(key, valueDel)
	return int64(len(key)) + valueDel.size()
}

func (k *KVTable) isEmpty() bool {
	return k.skl.Len() == 0
}

func (k *KVTable) iter() *MemTableIterator {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return newMemTableIterator(k.skl.Front())
}

func (k *KVTable) rangeFrom(start []byte) *MemTableIterator {
	k.mu.RLock()
	defer k.mu.RUnlock()
	elem := k.skl.Find(start)
	return newMemTableIterator(elem)
}

func (k *KVTable) awaitDurable() {
	<-k.durableNotify
}

func (k *KVTable) notifyDurable() {
	k.durableNotify <- "durable"
}

// ------------------------------------------------
// WritableKVTable
// ------------------------------------------------

type WritableKVTable struct {
	table *KVTable
	size  atomic.Int64
}

func newWritableKVTable() *WritableKVTable {
	return &WritableKVTable{
		table: newKVTable(),
	}
}

func (w *WritableKVTable) put(key, value []byte) {
	w.maybeSubtractOldValFromSize(key)
	newSize := w.table.put(key, value)
	w.size.Add(newSize)
}

func (w *WritableKVTable) delete(key []byte) {
	w.maybeSubtractOldValFromSize(key)
	newSize := w.table.delete(key)
	w.size.Add(newSize)
}

func (w *WritableKVTable) maybeSubtractOldValFromSize(key []byte) {
	oldDeletable, ok := w.table.get(key).Get()
	if ok {
		oldSize := int64(len(key)) + oldDeletable.size()
		w.size.Add(-oldSize)
	}
}

// ------------------------------------------------
// ImmutableWal
// ------------------------------------------------

type ImmutableWal struct {
	id    uint64
	table *KVTable
}

func newImmutableWal(id uint64, table *WritableKVTable) *ImmutableWal {
	return &ImmutableWal{
		id:    id,
		table: table.table,
	}
}

// ------------------------------------------------
// ImmutableMemtable
// ------------------------------------------------

type ImmutableMemtable struct {
	lastWalID   uint64
	table       *KVTable
	flushNotify chan string
}

func newImmutableMemtable(table *WritableKVTable, lastWalID uint64) *ImmutableMemtable {
	return &ImmutableMemtable{
		table:       table.table,
		lastWalID:   lastWalID,
		flushNotify: make(chan string),
	}
}

func (im *ImmutableMemtable) awaitFlushToL0() {
	<-im.flushNotify
}

func (im *ImmutableMemtable) notifyFlushToL0() {
	im.flushNotify <- "flush"
}

// ------------------------------------------------
// MemTableIterator
// ------------------------------------------------

type MemTableIterator struct {
	element *skiplist.Element
}

func newMemTableIterator(element *skiplist.Element) *MemTableIterator {
	return &MemTableIterator{
		element: element,
	}
}

func (m *MemTableIterator) Next() mo.Option[KeyValue] {
	for {
		keyVal, ok := m.NextEntry().Get()
		if ok {
			if keyVal.valueDel.isTombstone {
				continue
			}

			return mo.Some[KeyValue](KeyValue{
				key:   keyVal.key,
				value: keyVal.valueDel.value,
			})
		} else {
			return mo.None[KeyValue]()
		}
	}
}

func (m *MemTableIterator) NextEntry() mo.Option[KeyValueDeletable] {
	elem := m.element
	if elem == nil {
		return mo.None[KeyValueDeletable]()
	}

	m.element = m.element.Next()

	return mo.Some(KeyValueDeletable{
		key:      elem.Key().([]byte),
		valueDel: elem.Value.(ValueDeletable),
	})
}

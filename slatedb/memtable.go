package slatedb

import (
	"bytes"
	"github.com/huandu/skiplist"
	"github.com/naveen246/slatedb-go/slatedb/common"
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

func (k *KVTable) get(key []byte) mo.Option[common.ValueDeletable] {
	k.mu.RLock()
	defer k.mu.RUnlock()

	elem := k.skl.Get(key)
	if elem == nil {
		return mo.None[common.ValueDeletable]()
	}
	return mo.Some[common.ValueDeletable](elem.Value.(common.ValueDeletable))
}

func (k *KVTable) put(key []byte, value []byte) int64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	valueDel := common.ValueDeletable{
		Value:       value,
		IsTombstone: false,
	}
	k.skl.Set(key, valueDel)
	return int64(len(key)) + valueDel.Size()
}

func (k *KVTable) delete(key []byte) int64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	elem := k.skl.Get(key)
	if elem == nil {
		return 0
	}

	valueDel := common.ValueDeletable{
		Value:       nil,
		IsTombstone: true,
	}
	k.skl.Set(key, valueDel)
	return int64(len(key)) + valueDel.Size()
}

func (k *KVTable) isEmpty() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
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
		oldSize := int64(len(key)) + oldDeletable.Size()
		w.size.Add(-oldSize)
	}
}

// ------------------------------------------------
// ImmutableWAL
// ------------------------------------------------

type ImmutableWAL struct {
	id    uint64
	table *KVTable
}

func newImmutableWal(id uint64, table *WritableKVTable) ImmutableWAL {
	return ImmutableWAL{
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

func newImmutableMemtable(table *WritableKVTable, lastWalID uint64) ImmutableMemtable {
	return ImmutableMemtable{
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

func (iter *MemTableIterator) Next() (mo.Option[common.KeyValue], error) {
	for {
		entry, err := iter.NextEntry()
		if err != nil {
			return mo.None[common.KeyValue](), err
		}
		keyVal, ok := entry.Get()
		if ok {
			if keyVal.ValueDel.IsTombstone {
				continue
			}

			return mo.Some[common.KeyValue](common.KeyValue{
				Key:   keyVal.Key,
				Value: keyVal.ValueDel.Value,
			}), nil
		} else {
			return mo.None[common.KeyValue](), nil
		}
	}
}

func (iter *MemTableIterator) NextEntry() (mo.Option[common.KeyValueDeletable], error) {
	elem := iter.element
	if elem == nil {
		return mo.None[common.KeyValueDeletable](), nil
	}

	iter.element = iter.element.Next()

	return mo.Some(common.KeyValueDeletable{
		Key:      elem.Key().([]byte),
		ValueDel: elem.Value.(common.ValueDeletable),
	}), nil
}

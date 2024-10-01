package slatedb

import (
	"github.com/huandu/skiplist"
	"github.com/naveen246/slatedb-go/slatedb/common"
	"github.com/samber/mo"
	"math"
	"sync"
	"sync/atomic"
)

// ------------------------------------------------
// KVTable
// ------------------------------------------------

type KVTable struct {
	sync.RWMutex
	flushLock     *sync.Mutex
	flushNotifyCh chan chan bool

	// skl skipList stores key ([]byte), value (ValueDeletable) pairs
	skl *skiplist.SkipList
}

func newKVTable() *KVTable {
	return &KVTable{
		skl:           skiplist.New(skiplist.Bytes),
		flushLock:     &sync.Mutex{},
		flushNotifyCh: make(chan chan bool, math.MaxUint8),
	}
}

func (k *KVTable) get(key []byte) mo.Option[common.ValueDeletable] {
	k.RLock()
	defer k.RUnlock()

	elem := k.skl.Get(key)
	if elem == nil {
		return mo.None[common.ValueDeletable]()
	}
	return mo.Some(elem.Value.(common.ValueDeletable))
}

func (k *KVTable) put(key []byte, value []byte) int64 {
	k.Lock()
	defer k.Unlock()

	valueDel := common.ValueDeletable{
		Value:       value,
		IsTombstone: false,
	}
	k.skl.Set(key, valueDel)
	return int64(len(key)) + valueDel.Size()
}

func (k *KVTable) delete(key []byte) int64 {
	k.Lock()
	defer k.Unlock()

	valueDel := common.ValueDeletable{
		Value:       nil,
		IsTombstone: true,
	}
	k.skl.Set(key, valueDel)
	return int64(len(key)) + valueDel.Size()
}

func (k *KVTable) isEmpty() bool {
	k.RLock()
	defer k.RUnlock()
	return k.skl.Len() == 0
}

func (k *KVTable) iter() *MemTableIterator {
	k.RLock()
	defer k.RUnlock()
	return newMemTableIterator(k.skl.Front())
}

func (k *KVTable) rangeFrom(start []byte) *MemTableIterator {
	k.RLock()
	defer k.RUnlock()
	elem := k.skl.Find(start)
	return newMemTableIterator(elem)
}

// we do not expect this to block because flushNotifyCh has a length of math.MaxUint8
// and we do not expect that many clients waiting on awaitWALFlush.
// if this expectation changes in future there is a possibility of deadlock
func (k *KVTable) awaitWALFlush() <-chan bool {
	k.flushLock.Lock()
	defer k.flushLock.Unlock()
	done := make(chan bool, 1)
	k.flushNotifyCh <- done
	return done
}

func (k *KVTable) notifyWALFlushed() {
	k.flushLock.Lock()
	defer k.flushLock.Unlock()
	if len(k.flushNotifyCh) == 0 {
		return
	}
	for i := 0; i < len(k.flushNotifyCh); i++ {
		done := <-k.flushNotifyCh
		if len(done) == 0 {
			done <- true
			close(done)
		}
	}
	k.flushNotifyCh = make(chan chan bool, math.MaxUint8)
}

func (k *KVTable) clone() *KVTable {
	k.RLock()
	defer k.RUnlock()
	skl := skiplist.New(skiplist.Bytes)
	current := k.skl.Front()
	for current != nil {
		key := current.Key().([]byte)
		val := current.Value.(common.ValueDeletable)
		skl.Set(key, val)
		current = current.Next()
	}
	return &KVTable{
		RWMutex:       sync.RWMutex{},
		flushLock:     k.flushLock,
		flushNotifyCh: k.flushNotifyCh,
		skl:           skl,
	}
}

// ------------------------------------------------
// WritableKVTable
// ------------------------------------------------

type WritableKVTable struct {
	sync.RWMutex
	table *KVTable
	size  atomic.Int64
}

func newWritableKVTable() *WritableKVTable {
	return &WritableKVTable{
		table: newKVTable(),
	}
}

func (w *WritableKVTable) put(key, value []byte) {
	w.Lock()
	defer w.Unlock()
	w.maybeSubtractOldValFromSize(key)
	newSize := w.table.put(key, value)
	w.size.Add(newSize)
}

func (w *WritableKVTable) delete(key []byte) {
	w.Lock()
	defer w.Unlock()
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

func (w *WritableKVTable) clone() *WritableKVTable {
	w.RLock()
	defer w.RUnlock()
	kvTable := &WritableKVTable{
		RWMutex: sync.RWMutex{},
		table:   w.table.clone(),
		size:    atomic.Int64{},
	}
	kvTable.size.Store(w.size.Load())
	return kvTable
}

// ------------------------------------------------
// ImmutableWAL
// ------------------------------------------------

type ImmutableWAL struct {
	id    uint64
	table *KVTable
}

func newImmutableWal(id uint64, table *WritableKVTable) *ImmutableWAL {
	return &ImmutableWAL{
		id:    id,
		table: table.table,
	}
}

func (i *ImmutableWAL) clone() *ImmutableWAL {
	return &ImmutableWAL{
		id:    i.id,
		table: i.table.clone(),
	}
}

// ------------------------------------------------
// ImmutableMemtable
// ------------------------------------------------

type ImmutableMemtable struct {
	lastWalID uint64
	table     *KVTable
}

func newImmutableMemtable(table *WritableKVTable, lastWalID uint64) ImmutableMemtable {
	return ImmutableMemtable{
		table:     table.table,
		lastWalID: lastWalID,
	}
}

func (i *ImmutableMemtable) clone() *ImmutableMemtable {
	return &ImmutableMemtable{
		lastWalID: i.lastWalID,
		table:     i.table.clone(),
	}
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

func (iter *MemTableIterator) Next() (mo.Option[common.KV], error) {
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

func (iter *MemTableIterator) NextEntry() (mo.Option[common.KVDeletable], error) {
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

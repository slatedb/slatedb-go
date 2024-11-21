package table

import (
	"github.com/huandu/skiplist"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"sync/atomic"
)

// ------------------------------------------------
// KVTable
// ------------------------------------------------

type KVTable struct {
	// skl skipList stores key ([]byte), value (ValueDeletable) pairs
	skl *skiplist.SkipList

	// size of KVTable changes when we put/delete a key
	size atomic.Int64

	// Initially this KVTable is part of a WAL and clients wait on isDurableCh channel to know if the WAL is durably
	// committed to object store
	// The WALFlushTask goroutine converts the WAL to ImmutableWAL(backed by this same KVTable),
	// then flushes the ImmutableWAL to object store and
	// then closes this channel to notify clients waiting on isDurableCh channel
	isDurableCh chan bool
}

func newKVTable() *KVTable {
	return &KVTable{
		skl:         skiplist.New(skiplist.Bytes),
		isDurableCh: make(chan bool),
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

// AwaitWALFlush - This is called during DB.Put/DB.Delete to wait till the WAL is
// durably committed to object store
func (t *KVTable) AwaitWALFlush() {
	<-t.isDurableCh
}

// NotifyWALFlushed - This is called by WALFlushTask goroutine to notify any client waiting
// on AwaitWALFlush that the WAL contents have been durably committed to object store
func (t *KVTable) NotifyWALFlushed() {
	close(t.isDurableCh)
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
		isDurableCh: make(chan bool),
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

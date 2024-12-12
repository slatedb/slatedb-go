package table

import (
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/types"
	"sync"
)

// ------------------------------------------------
// WAL
// ------------------------------------------------

type WAL struct {
	sync.RWMutex
	table *KVTable
}

func NewWAL() *WAL {
	return &WAL{
		table: newKVTable(),
	}
}

func (w *WAL) Put(key []byte, value []byte) int64 {
	w.Lock()
	defer w.Unlock()
	return w.table.put(key, value)
}

func (w *WAL) Get(key []byte) mo.Option[types.Value] {
	w.RLock()
	defer w.RUnlock()
	return w.table.get(key)
}

func (w *WAL) Delete(key []byte) {
	w.Lock()
	defer w.Unlock()
	w.table.delete(key)
}

func (w *WAL) Table() *KVTable {
	w.RLock()
	defer w.RUnlock()
	return w.table
}

func (w *WAL) Size() int64 {
	w.RLock()
	defer w.RUnlock()
	return w.table.size.Load()
}

func (w *WAL) Iter() *KVTableIterator {
	w.RLock()
	defer w.RUnlock()
	return w.table.iter()
}

func (w *WAL) Clone() *WAL {
	w.RLock()
	defer w.RUnlock()

	return &WAL{
		table: w.table.clone(),
	}
}

// ------------------------------------------------
// ImmutableWAL
// ------------------------------------------------

type ImmutableWAL struct {
	sync.RWMutex
	id    uint64
	table *KVTable
}

func NewImmutableWAL(wal *WAL, id uint64) *ImmutableWAL {
	return &ImmutableWAL{
		id:    id,
		table: wal.table,
	}
}

func (iw *ImmutableWAL) Get(key []byte) mo.Option[types.Value] {
	iw.RLock()
	defer iw.RUnlock()
	return iw.table.get(key)
}

func (iw *ImmutableWAL) ID() uint64 {
	iw.RLock()
	defer iw.RUnlock()
	return iw.id
}

func (iw *ImmutableWAL) Table() *KVTable {
	iw.RLock()
	defer iw.RUnlock()
	return iw.table
}

func (iw *ImmutableWAL) Iter() *KVTableIterator {
	iw.RLock()
	defer iw.RUnlock()
	return iw.table.iter()
}

func (iw *ImmutableWAL) Clone() *ImmutableWAL {
	iw.RLock()
	defer iw.RUnlock()

	return &ImmutableWAL{
		id:    iw.id,
		table: iw.table.clone(),
	}
}

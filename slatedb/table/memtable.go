package table

import (
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"sync"
)

// ------------------------------------------------
// Memtable
// ------------------------------------------------

type Memtable struct {
	sync.RWMutex
	table *KVTable

	// As WALs get written to Memtable, this value holds the ID of the last WAL that was written to Memtable
	lastWalID mo.Option[uint64]
}

func NewMemtable() *Memtable {
	return &Memtable{
		table:     newKVTable(),
		lastWalID: mo.None[uint64](),
	}
}

func (m *Memtable) Put(key []byte, value []byte) {
	m.Lock()
	defer m.Unlock()
	m.table.put(key, value)
}

func (m *Memtable) Get(key []byte) mo.Option[common.ValueDeletable] {
	m.RLock()
	defer m.RUnlock()
	return m.table.get(key)
}

func (m *Memtable) Delete(key []byte) {
	m.Lock()
	defer m.Unlock()
	m.table.delete(key)
}

func (m *Memtable) Size() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.table.size.Load()
}

func (m *Memtable) LastWalID() mo.Option[uint64] {
	m.RLock()
	defer m.RUnlock()
	return m.lastWalID
}

func (m *Memtable) SetLastWalID(lastWalID mo.Option[uint64]) {
	m.Lock()
	defer m.Unlock()
	m.lastWalID = lastWalID
}

func (m *Memtable) Iter() *KVTableIterator {
	m.RLock()
	defer m.RUnlock()
	return m.table.iter()
}

func (m *Memtable) Clone() *Memtable {
	m.RLock()
	defer m.RUnlock()

	return &Memtable{
		table:     m.table.clone(),
		lastWalID: m.lastWalID,
	}
}

// ------------------------------------------------
// ImmutableMemtable
// ------------------------------------------------

type ImmutableMemtable struct {
	sync.RWMutex
	table     *KVTable
	lastWalID uint64
}

func NewImmutableMemtable(memtable *Memtable, lastWalID uint64) *ImmutableMemtable {
	return &ImmutableMemtable{
		table:     memtable.table,
		lastWalID: lastWalID,
	}
}

func (i *ImmutableMemtable) Get(key []byte) mo.Option[common.ValueDeletable] {
	i.RLock()
	defer i.RUnlock()
	return i.table.get(key)
}

func (i *ImmutableMemtable) LastWalID() uint64 {
	i.RLock()
	defer i.RUnlock()
	return i.lastWalID
}

func (i *ImmutableMemtable) Iter() *KVTableIterator {
	i.RLock()
	defer i.RUnlock()
	return i.table.iter()
}

func (i *ImmutableMemtable) clone() *ImmutableMemtable {
	i.RLock()
	defer i.RUnlock()
	return &ImmutableMemtable{
		table:     i.table.clone(),
		lastWalID: i.lastWalID,
	}
}

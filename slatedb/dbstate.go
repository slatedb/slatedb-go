package slatedb

import (
	"bytes"
	"fmt"
	"github.com/gammazero/deque"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"sync"
)

// ------------------------------------------------
// DBState
// ------------------------------------------------

type DBState struct {
	sync.RWMutex
	memtable *WritableKVTable
	wal      *WritableKVTable
	state    *COWDBState
}

type COWDBState struct {
	immMemtable *deque.Deque[ImmutableMemtable]
	immWAL      *deque.Deque[ImmutableWAL]
	core        *CoreDBState
}

type CoreDBState struct {
	l0LastCompacted       mo.Option[ulid.ULID]
	l0                    *deque.Deque[SSTableHandle]
	compacted             []SortedRun
	nextWalSstID          uint64
	lastCompactedWalSSTID uint64
}

type DBStateSnapshot struct {
	memtable *KVTable
	wal      *KVTable
	state    *COWDBState
}

func newCoreDBState() *CoreDBState {
	return &CoreDBState{
		l0LastCompacted:       mo.None[ulid.ULID](),
		l0:                    deque.New[SSTableHandle](0),
		compacted:             []SortedRun{},
		nextWalSstID:          1,
		lastCompactedWalSSTID: 0,
	}
}

func newDBState(coreDBState *CoreDBState) *DBState {
	return &DBState{
		memtable: newWritableKVTable(),
		wal:      newWritableKVTable(),
		state: &COWDBState{
			immMemtable: deque.New[ImmutableMemtable](0),
			immWAL:      deque.New[ImmutableWAL](0),
			core:        coreDBState,
		},
	}
}

func (s *DBState) snapshot() *DBStateSnapshot {
	s.RLock()
	defer s.RUnlock()

	return &DBStateSnapshot{
		memtable: s.memtable.table,
		wal:      s.wal.table,
		state:    s.state,
	}
}

func (s *DBState) freezeMemtable(walID uint64) {
	s.Lock()
	defer s.Unlock()

	oldMemtable := s.memtable
	immMemtable := newImmutableMemtable(oldMemtable, walID)

	s.memtable = newWritableKVTable()
	s.state.immMemtable.PushFront(immMemtable)
}

func (s *DBState) freezeWAL() mo.Option[uint64] {
	s.Lock()
	defer s.Unlock()

	if s.wal.table.isEmpty() {
		return mo.None[uint64]()
	}

	oldWAL := s.wal
	immWAL := newImmutableWal(s.state.core.nextWalSstID, oldWAL)

	s.wal = newWritableKVTable()
	s.state.immWAL.PushFront(immWAL)
	s.state.core.nextWalSstID += 1

	return mo.Some(immWAL.id)
}

func (s *DBState) popImmWAL() {
	s.Lock()
	defer s.Unlock()

	s.state.immWAL.PopBack()
}

func (s *DBState) moveImmMemtableToL0(immMemtable ImmutableMemtable, sstHandle SSTableHandle) {
	s.Lock()
	defer s.Unlock()

	popped := s.state.immMemtable.PopBack()
	assertTrue(popped.lastWalID == immMemtable.lastWalID, "")

	s.state.core.l0.PushFront(sstHandle)
	s.state.core.lastCompactedWalSSTID = immMemtable.lastWalID
}

func (s *DBState) incrementNextWALID() {
	s.Lock()
	defer s.Unlock()

	s.state.core.nextWalSstID += 1
}

func (s *DBState) refreshDBState(compactorState *CoreDBState) {
	s.Lock()
	defer s.Unlock()

	// copy over L0 up to l0_last_compacted
	l0LastCompacted := compactorState.l0LastCompacted
	newL0 := deque.New[SSTableHandle](0)
	for i := 0; i < s.state.core.l0.Len(); i++ {
		sst := s.state.core.l0.At(i)
		if l0LastCompacted.IsPresent() {
			lastCompacted, _ := l0LastCompacted.Get()
			if sst.id.typ == Compacted && sst.id.value == lastCompacted.String() {
				break
			}
		}
		newL0.PushBack(sst)
	}

	s.state.core.l0LastCompacted = l0LastCompacted
	s.state.core.l0 = newL0
	s.state.core.compacted = compactorState.compacted
}

// ------------------------------------------------
// SSTableID
// ------------------------------------------------

type SSTableIDType int

const (
	WAL SSTableIDType = iota
	Compacted
)

type SSTableID struct {
	typ   SSTableIDType
	value string
}

func newSSTableIDWal(id uint64) SSTableID {
	return SSTableID{typ: WAL, value: fmt.Sprintf("%020d", id)}
}

func newSSTableIDCompacted(id ulid.ULID) SSTableID {
	return SSTableID{typ: Compacted, value: id.String()}
}

// ------------------------------------------------
// SSTableHandle
// ------------------------------------------------

type SSTableHandle struct {
	id   SSTableID
	info *SSTableInfoOwned
}

func newSSTableHandle(id SSTableID, info *SSTableInfoOwned) *SSTableHandle {
	return &SSTableHandle{id, info}
}

func (h *SSTableHandle) rangeCoversKey(key []byte) bool {
	firstKey := h.info.borrow().FirstKeyBytes()
	return firstKey != nil && bytes.Compare(key, firstKey) >= 0
}

package slatedb

import (
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/table"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/gammazero/deque"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

// ------------------------------------------------
// DBState
// ------------------------------------------------

// CoreDBState is the DB state that gets read/written to Manifest stored on object store
type CoreDBState struct {
	l0LastCompacted mo.Option[ulid.ULID]
	l0              []sstable.Handle
	compacted       []SortedRun

	// nextWalSstID is used as the ID of new ImmutableWAL created during WAL flush process
	// It is initialized to 1 and keeps getting incremented by 1 for new ImmutableWALs
	nextWalSstID atomic.Uint64

	// lastCompactedWalSSTID is the ID of the last ImmutableWAL that has been flushed to Memtable and then to Level0 of object store.
	// This value is updated when Memtable is flushed to Level0 of object store.
	// It is later used during crash recovery to recover only those WALs that have not yet been flushed to Level0.
	lastCompactedWalSSTID atomic.Uint64
}

func newCoreDBState() *CoreDBState {
	coreState := &CoreDBState{
		l0LastCompacted: mo.None[ulid.ULID](),
		l0:              make([]sstable.Handle, 0),
		compacted:       []SortedRun{},
	}
	coreState.nextWalSstID.Store(1)
	coreState.lastCompactedWalSSTID.Store(0)
	return coreState
}

func (c *CoreDBState) clone() *CoreDBState {
	l0 := make([]sstable.Handle, 0, len(c.l0))
	for _, sst := range c.l0 {
		l0 = append(l0, *sst.Clone())
	}
	compacted := make([]SortedRun, 0, len(c.compacted))
	for _, sr := range c.compacted {
		compacted = append(compacted, *sr.clone())
	}
	coreState := &CoreDBState{
		l0LastCompacted: c.l0LastCompacted,
		l0:              l0,
		compacted:       compacted,
	}
	coreState.nextWalSstID.Store(c.nextWalSstID.Load())
	coreState.lastCompactedWalSSTID.Store(c.lastCompactedWalSSTID.Load())
	return coreState
}

func LogState(log *slog.Logger, c *CoreDBState) {
	log.Info("DB Levels:")
	log.Info("-----------------")
	log.Info("state", "L0", c.l0)
	log.Info("state", "compacted", c.compacted)
	log.Info("-----------------")
}

// DBStateSnapshot contains state required for read methods (eg. GET)
type DBStateSnapshot struct {
	wal          *table.WAL
	memtable     *table.Memtable
	immWALs      *deque.Deque[*table.ImmutableWAL]
	immMemtables *deque.Deque[*table.ImmutableMemtable]
	core         *CoreDBState
}

type DBState struct {
	sync.RWMutex
	wal          *table.WAL
	memtable     *table.Memtable
	immWALs      *deque.Deque[*table.ImmutableWAL]
	immMemtables *deque.Deque[*table.ImmutableMemtable]
	core         *CoreDBState
}

func newDBState(coreDBState *CoreDBState) *DBState {
	return &DBState{
		wal:          table.NewWAL(),
		memtable:     table.NewMemtable(),
		immWALs:      deque.New[*table.ImmutableWAL](0),
		immMemtables: deque.New[*table.ImmutableMemtable](0),
		core:         coreDBState,
	}
}

func (s *DBState) WAL() *table.WAL {
	s.RLock()
	defer s.RUnlock()
	return s.wal
}

func (s *DBState) Memtable() *table.Memtable {
	s.RLock()
	defer s.RUnlock()
	return s.memtable
}

func (s *DBState) ImmWALs() *deque.Deque[*table.ImmutableWAL] {
	s.RLock()
	defer s.RUnlock()
	return s.immWALs
}

func (s *DBState) L0LastCompacted() mo.Option[ulid.ULID] {
	s.RLock()
	defer s.RUnlock()
	return s.core.l0LastCompacted
}

func (s *DBState) L0() []sstable.Handle {
	s.RLock()
	defer s.RUnlock()
	return s.core.l0
}

func (s *DBState) NextWALID() uint64 {
	return s.core.nextWalSstID.Load()
}

func (s *DBState) LastCompactedWALID() uint64 {
	return s.core.lastCompactedWalSSTID.Load()
}

func (s *DBState) PutKVToWAL(key []byte, value []byte) *table.WAL {
	s.Lock()
	defer s.Unlock()
	s.wal.Put(key, value)
	return s.wal
}

func (s *DBState) DeleteKVFromWAL(key []byte) *table.WAL {
	s.Lock()
	defer s.Unlock()
	s.wal.Delete(key)
	return s.wal
}

func (s *DBState) PutKVToMemtable(key []byte, value []byte) {
	s.Lock()
	defer s.Unlock()
	s.memtable.Put(key, value)
}

func (s *DBState) DeleteKVFromMemtable(key []byte) {
	s.Lock()
	defer s.Unlock()
	s.memtable.Delete(key)
}

func (s *DBState) coreStateClone() *CoreDBState {
	s.RLock()
	defer s.RUnlock()
	return s.core.clone()
}

func (s *DBState) snapshot() *DBStateSnapshot {
	s.RLock()
	defer s.RUnlock()

	return &DBStateSnapshot{
		wal:          s.wal.Clone(),
		memtable:     s.memtable.Clone(),
		immWALs:      common.CopyDeque(s.immWALs),
		immMemtables: common.CopyDeque(s.immMemtables),
		core:         s.core.clone(),
	}
}

func (s *DBState) freezeMemtable(walID uint64) {
	s.Lock()
	defer s.Unlock()

	oldMemtable := s.memtable
	immMemtable := table.NewImmutableMemtable(oldMemtable, walID)

	s.memtable = table.NewMemtable()
	s.immMemtables.PushFront(immMemtable)
}

func (s *DBState) freezeWAL() mo.Option[uint64] {
	s.Lock()
	defer s.Unlock()

	if s.wal.Size() == 0 {
		return mo.None[uint64]()
	}

	oldWAL := s.wal
	immWAL := table.NewImmutableWAL(oldWAL, s.core.nextWalSstID.Load())
	s.wal = table.NewWAL()
	s.immWALs.PushFront(immWAL)
	s.core.nextWalSstID.Add(1)

	return mo.Some(immWAL.ID())
}

func (s *DBState) popImmWAL() {
	s.Lock()
	defer s.Unlock()

	s.immWALs.PopBack()
}

func (s *DBState) oldestImmWAL() mo.Option[*table.ImmutableWAL] {
	s.RLock()
	defer s.RUnlock()

	if s.immWALs.Len() == 0 {
		return mo.None[*table.ImmutableWAL]()
	}
	return mo.Some(s.immWALs.Back())
}

func (s *DBState) oldestImmMemtable() mo.Option[*table.ImmutableMemtable] {
	s.RLock()
	defer s.RUnlock()

	if s.immMemtables.Len() == 0 {
		return mo.None[*table.ImmutableMemtable]()
	}
	return mo.Some(s.immMemtables.Back())
}

func (s *DBState) moveImmMemtableToL0(immMemtable *table.ImmutableMemtable, sstHandle *sstable.Handle) {
	s.Lock()
	defer s.Unlock()

	popped := s.immMemtables.PopBack()
	assert.True(popped.LastWalID() == immMemtable.LastWalID(), "")

	s.core.l0 = append([]sstable.Handle{*sstHandle}, s.core.l0...)
	s.core.lastCompactedWalSSTID.Store(immMemtable.LastWalID())
}

func (s *DBState) incrementNextWALID() {
	s.core.nextWalSstID.Add(1)
}

func (s *DBState) refreshDBState(compactorState *CoreDBState) {
	s.Lock()
	defer s.Unlock()

	// copy over L0 up to l0LastCompacted
	l0LastCompacted := compactorState.l0LastCompacted
	newL0 := make([]sstable.Handle, 0)
	for i := 0; i < len(s.core.l0); i++ {
		sst := s.core.l0[i]
		if l0LastCompacted.IsPresent() {
			lastCompacted, _ := l0LastCompacted.Get()
			if sst.Id.Type == sstable.Compacted && sst.Id.Value == lastCompacted.String() {
				break
			}
		}
		newL0 = append(newL0, sst)
	}

	s.core.l0LastCompacted = l0LastCompacted
	s.core.l0 = newL0
	s.core.compacted = compactorState.compacted
}

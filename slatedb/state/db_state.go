package state

import (
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/levels"
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
	compacted       []levels.SortedRun

	// nextWalSstID is used as the ID of new ImmutableWAL created during WAL flush process
	// It is initialized to 1 and keeps getting incremented by 1 for new ImmutableWALs
	nextWalSstID atomic.Uint64

	// lastCompactedWalSSTID is the ID of the last ImmutableWAL that has been flushed to Memtable and then to Level0 of object store.
	// This value is updated when Memtable is flushed to Level0 of object store.
	// It is later used during crash recovery to recover only those WALs that have not yet been flushed to Level0.
	lastCompactedWalSSTID atomic.Uint64
}

type CoreStateSnapshot struct {
	L0LastCompacted       mo.Option[ulid.ULID]
	L0                    []sstable.Handle
	Compacted             []levels.SortedRun
	NextWalSstID          atomic.Uint64
	LastCompactedWalSSTID atomic.Uint64
}

func (s *CoreStateSnapshot) ToCoreState() *CoreDBState {
	snapshot := s.Clone()
	coreState := &CoreDBState{
		l0LastCompacted: snapshot.L0LastCompacted,
		l0:              snapshot.L0,
		compacted:       snapshot.Compacted,
	}
	coreState.nextWalSstID.Store(s.NextWalSstID.Load())
	coreState.lastCompactedWalSSTID.Store(s.LastCompactedWalSSTID.Load())
	return coreState
}

func (s *CoreStateSnapshot) Clone() *CoreStateSnapshot {
	l0 := make([]sstable.Handle, 0, len(s.L0))
	for _, sst := range s.L0 {
		l0 = append(l0, *sst.Clone())
	}
	compacted := make([]levels.SortedRun, 0, len(s.Compacted))
	for _, sr := range s.Compacted {
		compacted = append(compacted, *sr.Clone())
	}
	snapshot := &CoreStateSnapshot{
		L0LastCompacted: s.L0LastCompacted,
		L0:              l0,
		Compacted:       compacted,
	}
	snapshot.NextWalSstID.Store(s.NextWalSstID.Load())
	snapshot.LastCompactedWalSSTID.Store(s.LastCompactedWalSSTID.Load())
	return snapshot
}

func NewCoreDBState() *CoreDBState {
	coreState := &CoreDBState{
		l0LastCompacted: mo.None[ulid.ULID](),
		l0:              make([]sstable.Handle, 0),
		compacted:       []levels.SortedRun{},
	}
	coreState.nextWalSstID.Store(1)
	coreState.lastCompactedWalSSTID.Store(0)
	return coreState
}

func (c *CoreDBState) Snapshot() *CoreStateSnapshot {
	l0 := make([]sstable.Handle, 0, len(c.l0))
	for _, sst := range c.l0 {
		l0 = append(l0, *sst.Clone())
	}
	compacted := make([]levels.SortedRun, 0, len(c.compacted))
	for _, sr := range c.compacted {
		compacted = append(compacted, *sr.Clone())
	}
	coreState := &CoreStateSnapshot{
		L0LastCompacted: c.l0LastCompacted,
		L0:              l0,
		Compacted:       compacted,
	}
	coreState.NextWalSstID.Store(c.nextWalSstID.Load())
	coreState.LastCompactedWalSSTID.Store(c.lastCompactedWalSSTID.Load())
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
	Wal          *table.WAL
	Memtable     *table.Memtable
	ImmWALs      *deque.Deque[*table.ImmutableWAL]
	ImmMemtables *deque.Deque[*table.ImmutableMemtable]
	Core         *CoreStateSnapshot
}

type DBState struct {
	sync.RWMutex
	wal          *table.WAL
	memtable     *table.Memtable
	immWALs      *deque.Deque[*table.ImmutableWAL]
	immMemtables *deque.Deque[*table.ImmutableMemtable]
	core         *CoreDBState
}

func NewDBState(coreDBState *CoreDBState) *DBState {
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

func (s *DBState) CoreStateSnapshot() *CoreStateSnapshot {
	s.RLock()
	defer s.RUnlock()
	return s.core.Snapshot()
}

func (s *DBState) Snapshot() *DBStateSnapshot {
	s.RLock()
	defer s.RUnlock()

	return &DBStateSnapshot{
		Wal:          s.wal.Clone(),
		Memtable:     s.memtable.Clone(),
		ImmWALs:      common.CopyDeque(s.immWALs),
		ImmMemtables: common.CopyDeque(s.immMemtables),
		Core:         s.core.Snapshot(),
	}
}

func (s *DBState) FreezeMemtable(walID uint64) {
	s.Lock()
	defer s.Unlock()

	oldMemtable := s.memtable
	immMemtable := table.NewImmutableMemtable(oldMemtable, walID)

	s.memtable = table.NewMemtable()
	s.immMemtables.PushFront(immMemtable)
}

func (s *DBState) FreezeWAL() mo.Option[uint64] {
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

func (s *DBState) PopImmWAL() {
	s.Lock()
	defer s.Unlock()

	s.immWALs.PopBack()
}

func (s *DBState) OldestImmWAL() mo.Option[*table.ImmutableWAL] {
	s.RLock()
	defer s.RUnlock()

	if s.immWALs.Len() == 0 {
		return mo.None[*table.ImmutableWAL]()
	}
	return mo.Some(s.immWALs.Back())
}

func (s *DBState) OldestImmMemtable() mo.Option[*table.ImmutableMemtable] {
	s.RLock()
	defer s.RUnlock()

	if s.immMemtables.Len() == 0 {
		return mo.None[*table.ImmutableMemtable]()
	}
	return mo.Some(s.immMemtables.Back())
}

func (s *DBState) MoveImmMemtableToL0(immMemtable *table.ImmutableMemtable, sstHandle *sstable.Handle) {
	s.Lock()
	defer s.Unlock()

	popped := s.immMemtables.PopBack()
	assert.True(popped.LastWalID() == immMemtable.LastWalID(), "")

	s.core.l0 = append([]sstable.Handle{*sstHandle}, s.core.l0...)
	s.core.lastCompactedWalSSTID.Store(immMemtable.LastWalID())
}

func (s *DBState) IncrementNextWALID() {
	s.core.nextWalSstID.Add(1)
}

func (s *DBState) RefreshDBState(compactorState *CoreStateSnapshot) {
	s.Lock()
	defer s.Unlock()

	// copy over L0 up to l0LastCompacted
	l0LastCompacted := compactorState.L0LastCompacted
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
	s.core.compacted = compactorState.Compacted
}

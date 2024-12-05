package slatedb

import (
	"bytes"
	"fmt"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/table"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/gammazero/deque"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"go.uber.org/zap"
)

// ------------------------------------------------
// DBState
// ------------------------------------------------

// CoreDBState is the DB state that gets read/written to Manifest stored on object store
type CoreDBState struct {
	l0LastCompacted mo.Option[ulid.ULID]
	l0              []SSTableHandle
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
		l0:              make([]SSTableHandle, 0),
		compacted:       []SortedRun{},
	}
	coreState.nextWalSstID.Store(1)
	coreState.lastCompactedWalSSTID.Store(0)
	return coreState
}

func (c *CoreDBState) clone() *CoreDBState {
	l0 := make([]SSTableHandle, 0, len(c.l0))
	for _, sst := range c.l0 {
		l0 = append(l0, *sst.clone())
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

func (c *CoreDBState) logState() {
	logger.Info("DB Levels:")
	logger.Info("-----------------")
	logger.Info("state", zap.Any("L0", c.l0))
	logger.Info("state", zap.Any("compacted", c.compacted))
	logger.Info("-----------------")
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

func (s *DBState) L0() []SSTableHandle {
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

func (s *DBState) moveImmMemtableToL0(immMemtable *table.ImmutableMemtable, sstHandle *SSTableHandle) {
	s.Lock()
	defer s.Unlock()

	popped := s.immMemtables.PopBack()
	common.AssertTrue(popped.LastWalID() == immMemtable.LastWalID(), "")

	s.core.l0 = append([]SSTableHandle{*sstHandle}, s.core.l0...)
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
	newL0 := make([]SSTableHandle, 0)
	for i := 0; i < len(s.core.l0); i++ {
		sst := s.core.l0[i]
		if l0LastCompacted.IsPresent() {
			lastCompacted, _ := l0LastCompacted.Get()
			if sst.id.typ == Compacted && sst.id.value == lastCompacted.String() {
				break
			}
		}
		newL0 = append(newL0, sst)
	}

	s.core.l0LastCompacted = l0LastCompacted
	s.core.l0 = newL0
	s.core.compacted = compactorState.compacted
}

// ------------------------------------------------
// SSTableID
// ------------------------------------------------

type SSTableIDType int

const (
	WAL SSTableIDType = iota + 1
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

func (s *SSTableID) walID() mo.Option[uint64] {
	if s.typ != WAL {
		return mo.None[uint64]()
	}

	val, err := strconv.Atoi(s.value)
	if err != nil {
		logger.Error("unable to parse table id", zap.Error(err))
		return mo.None[uint64]()
	}

	return mo.Some(uint64(val))
}

func (s *SSTableID) compactedID() mo.Option[ulid.ULID] {
	if s.typ != Compacted {
		return mo.None[ulid.ULID]()
	}

	val, err := ulid.Parse(s.value)
	if err != nil {
		logger.Error("unable to parse table id", zap.Error(err))
		return mo.None[ulid.ULID]()
	}

	return mo.Some(val)
}

func (s *SSTableID) clone() SSTableID {
	var sstID SSTableID
	if s.typ == WAL {
		id, _ := s.walID().Get()
		sstID = newSSTableIDWal(id)
	} else if s.typ == Compacted {
		id, _ := s.compactedID().Get()
		sstID = newSSTableIDCompacted(id)
	}
	return sstID
}

// ------------------------------------------------
// SSTableHandle
// ------------------------------------------------

// SSTableHandle represents the SSTable
type SSTableHandle struct {
	id   SSTableID
	info *sstable.SSTableInfo
}

func newSSTableHandle(id SSTableID, info *sstable.SSTableInfo) *SSTableHandle {
	return &SSTableHandle{id, info}
}

func (h *SSTableHandle) rangeCoversKey(key []byte) bool {
	if h.info.FirstKey.IsAbsent() {
		return false
	}
	firstKey, _ := h.info.FirstKey.Get()
	return firstKey != nil && bytes.Compare(key, firstKey) >= 0
}

func (h *SSTableHandle) clone() *SSTableHandle {
	return &SSTableHandle{
		id:   h.id.clone(),
		info: h.info.Clone(),
	}
}

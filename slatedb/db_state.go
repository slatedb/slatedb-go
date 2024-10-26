package slatedb

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"

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

type COWDBState struct {
	immMemtable *deque.Deque[ImmutableMemtable]
	immWAL      *deque.Deque[ImmutableWAL]
	core        *CoreDBState
}

func (s *COWDBState) clone() *COWDBState {
	return &COWDBState{
		immMemtable: common.CopyDeque(s.immMemtable),
		immWAL:      common.CopyDeque(s.immWAL),
		core:        s.core.clone(),
	}
}

type CoreDBState struct {
	l0LastCompacted       mo.Option[ulid.ULID]
	l0                    []SSTableHandle
	compacted             []SortedRun
	nextWalSstID          uint64
	lastCompactedWalSSTID uint64
}

func newCoreDBState() *CoreDBState {
	return &CoreDBState{
		l0LastCompacted:       mo.None[ulid.ULID](),
		l0:                    make([]SSTableHandle, 0),
		compacted:             []SortedRun{},
		nextWalSstID:          1,
		lastCompactedWalSSTID: 0,
	}
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
	return &CoreDBState{
		l0LastCompacted:       c.l0LastCompacted,
		l0:                    l0,
		compacted:             compacted,
		nextWalSstID:          c.nextWalSstID,
		lastCompactedWalSSTID: c.lastCompactedWalSSTID,
	}
}

func (c *CoreDBState) logState() {
	logger.Info("DB Levels:")
	logger.Info("-----------------")
	logger.Info("state", zap.Any("L0", c.l0))
	logger.Info("state", zap.Any("compacted", c.compacted))
	logger.Info("-----------------")
}

type DBStateSnapshot struct {
	memtable *KVTable
	wal      *KVTable
	state    *COWDBState
}

type DBState struct {
	sync.RWMutex
	memtable *WritableKVTable
	wal      *WritableKVTable
	state    *COWDBState
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

func (s *DBState) getState() *COWDBState {
	s.RLock()
	defer s.RUnlock()
	return s.state.clone()
}

func (s *DBState) snapshot() *DBStateSnapshot {
	s.RLock()
	defer s.RUnlock()

	return &DBStateSnapshot{
		memtable: s.memtable.table.clone(),
		wal:      s.wal.table.clone(),
		state:    s.state.clone(),
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
	s.state.immWAL.PushFront(*immWAL)
	s.state.core.nextWalSstID += 1

	return mo.Some(immWAL.id)
}

func (s *DBState) popImmWAL() {
	s.Lock()
	defer s.Unlock()

	s.state.immWAL.PopBack()
}

func (s *DBState) moveImmMemtableToL0(immMemtable ImmutableMemtable, sstHandle *SSTableHandle) {
	s.Lock()
	defer s.Unlock()

	popped := s.state.immMemtable.PopBack()
	common.AssertTrue(popped.lastWalID == immMemtable.lastWalID, "")

	s.state.core.l0 = append([]SSTableHandle{*sstHandle}, s.state.core.l0...)
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
	newL0 := make([]SSTableHandle, 0)
	for i := 0; i < len(s.state.core.l0); i++ {
		sst := s.state.core.l0[i]
		if l0LastCompacted.IsPresent() {
			lastCompacted, _ := l0LastCompacted.Get()
			if sst.id.typ == Compacted && sst.id.value == lastCompacted.String() {
				break
			}
		}
		newL0 = append(newL0, sst)
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

func (h *SSTableHandle) clone() *SSTableHandle {
	return &SSTableHandle{
		id:   h.id.clone(),
		info: h.info.clone(),
	}
}

type RowFeature int8

const (
	Flags RowFeature = iota
	Timestamp
)

type SSTableInfo struct {
	firstKey         mo.Option[[]byte]
	indexOffset      uint64
	indexLen         uint64
	filterOffset     uint64
	filterLen        uint64
	compressionCodec CompressionCodec
	rowFeatures      []RowFeature
}

type SsTableInfoCodec interface {
	encode(manifest SSTableInfo) []byte
	decode(data []byte) (SSTableInfo, error)
}

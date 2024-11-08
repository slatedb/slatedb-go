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
	l0LastCompacted mo.Option[ulid.ULID]
	l0              []SSTableHandle
	compacted       []SortedRun

	// nextWalSstID is used as the ID of new ImmutableWAL created during WAL flush process
	// It is initialized to 1 and keeps getting incremented by 1 for new ImmutableWALs
	nextWalSstID uint64

	// lastCompactedWalSSTID is the ID of the last ImmutableWAL that has been flushed to Memtable and then to Level0 of object store.
	// This value is updated when Memtable is flushed to Level0 of object store.
	// It is later used during crash recovery to recover only those WALs that have not yet been flushed to Level0.
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
	memtable *Memtable
	wal      *WritableKVTable
	state    *COWDBState
}

func newDBState(coreDBState *CoreDBState) *DBState {
	return &DBState{
		memtable: newMemtable(),
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
	immMemtable := newImmutableMemtable(oldMemtable.WritableKVTable, walID)

	s.memtable = newMemtable()
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

// SSTableHandle represents the SSTable
type SSTableHandle struct {
	id   SSTableID
	info *SSTableInfo
}

func newSSTableHandle(id SSTableID, info *SSTableInfo) *SSTableHandle {
	return &SSTableHandle{id, info}
}

func (h *SSTableHandle) rangeCoversKey(key []byte) bool {
	if h.info.firstKey.IsAbsent() {
		return false
	}
	firstKey, _ := h.info.firstKey.Get()
	return firstKey != nil && bytes.Compare(key, firstKey) >= 0
}

func (h *SSTableHandle) clone() *SSTableHandle {
	return &SSTableHandle{
		id:   h.id.clone(),
		info: h.info.clone(),
	}
}

// SSTableInfo contains meta information on the SSTable when it is serialized.
// This is used when we read SSTable as a slice of bytes from object storage and we want to parse the slice of bytes
// Each SSTable is a list of blocks and each block is a list of KeyValue pairs.
type SSTableInfo struct {
	// contains the firstKey of the SSTable
	firstKey mo.Option[[]byte]

	// the offset at which SSTableIndex starts when SSTable is serialized.
	// SSTableIndex holds the meta info about each block. SSTableIndex is defined in schemas/sst.fbs
	indexOffset uint64

	// the length of the SSTableIndex.
	indexLen uint64

	// the offset at which Bloom filter starts when SSTable is serialized.
	filterOffset uint64

	// the length of the Bloom filter
	filterLen uint64

	// the codec used to compress/decompress SSTable before writing/reading from object storage
	compressionCodec CompressionCodec
}

func (info *SSTableInfo) clone() *SSTableInfo {
	firstKey := mo.None[[]byte]()
	if info.firstKey.IsPresent() {
		key, _ := info.firstKey.Get()
		k := make([]byte, len(key))
		copy(k, key)
		firstKey = mo.Some(k)
	}
	return &SSTableInfo{
		firstKey:         firstKey,
		indexOffset:      info.indexOffset,
		indexLen:         info.indexLen,
		filterOffset:     info.filterOffset,
		filterLen:        info.filterLen,
		compressionCodec: info.compressionCodec,
	}
}

// SsTableInfoCodec - implementation of this interface defines how we
// encode SSTableInfo to byte slice and decode byte slice back to SSTableInfo
// Currently we use FlatBuffers for encoding and decoding.
type SsTableInfoCodec interface {
	encode(info *SSTableInfo) []byte
	decode(data []byte) *SSTableInfo
}

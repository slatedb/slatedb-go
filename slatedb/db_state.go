package slatedb

import (
	"bytes"
	"fmt"
	"github.com/slatedb/slatedb-go/slatedb/table"
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

// CoreDBState is the DB state that gets read/written to Manifest stored on object store
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

// DBStateSnapshot contains state required for read methods (eg. GET)
type DBStateSnapshot struct {
	wal         *table.WAL
	memtable    *table.Memtable
	immWAL      *deque.Deque[*table.ImmutableWAL]
	immMemtable *deque.Deque[*table.ImmutableMemtable]
	core        *CoreDBState
}

type DBState struct {
	sync.RWMutex
	wal         *table.WAL
	memtable    *table.Memtable
	immWAL      *deque.Deque[*table.ImmutableWAL]
	immMemtable *deque.Deque[*table.ImmutableMemtable]
	core        *CoreDBState
}

func newDBState(coreDBState *CoreDBState) *DBState {
	return &DBState{
		wal:         table.NewWAL(),
		memtable:    table.NewMemtable(),
		immWAL:      deque.New[*table.ImmutableWAL](0),
		immMemtable: deque.New[*table.ImmutableMemtable](0),
		core:        coreDBState,
	}
}

func (s *DBState) getCore() *CoreDBState {
	s.RLock()
	defer s.RUnlock()
	return s.core.clone()
}

func (s *DBState) snapshot() *DBStateSnapshot {
	s.RLock()
	defer s.RUnlock()

	return &DBStateSnapshot{
		wal:         s.wal.Clone(),
		memtable:    s.memtable.Clone(),
		immWAL:      common.CopyDeque(s.immWAL),
		immMemtable: common.CopyDeque(s.immMemtable),
		core:        s.core.clone(),
	}
}

func (s *DBState) freezeMemtable(walID uint64) {
	s.Lock()
	defer s.Unlock()

	oldMemtable := s.memtable
	immMemtable := table.NewImmutableMemtable(oldMemtable, walID)

	s.memtable = table.NewMemtable()
	s.immMemtable.PushFront(immMemtable)
}

func (s *DBState) freezeWAL() mo.Option[uint64] {
	s.Lock()
	defer s.Unlock()

	if s.wal.Size() == 0 {
		return mo.None[uint64]()
	}

	oldWAL := s.wal
	immWAL := table.NewImmutableWal(oldWAL, s.core.nextWalSstID)
	s.wal = table.NewWAL()
	s.immWAL.PushFront(immWAL)
	s.core.nextWalSstID += 1

	return mo.Some(immWAL.ID())
}

func (s *DBState) popImmWAL() {
	s.Lock()
	defer s.Unlock()

	s.immWAL.PopBack()
}

func (s *DBState) oldestImmWAL() mo.Option[*table.ImmutableWAL] {
	s.RLock()
	defer s.RUnlock()

	if s.immWAL.Len() == 0 {
		return mo.None[*table.ImmutableWAL]()
	}
	return mo.Some(s.immWAL.Back())
}

func (s *DBState) oldestImmMemtable() mo.Option[*table.ImmutableMemtable] {
	s.RLock()
	defer s.RUnlock()

	if s.immMemtable.Len() == 0 {
		return mo.None[*table.ImmutableMemtable]()
	}
	return mo.Some(s.immMemtable.Back())
}

func (s *DBState) moveImmMemtableToL0(immMemtable *table.ImmutableMemtable, sstHandle *SSTableHandle) {
	s.Lock()
	defer s.Unlock()

	popped := s.immMemtable.PopBack()
	common.AssertTrue(popped.LastWalID() == immMemtable.LastWalID(), "")

	s.core.l0 = append([]SSTableHandle{*sstHandle}, s.core.l0...)
	s.core.lastCompactedWalSSTID = immMemtable.LastWalID()
}

func (s *DBState) incrementNextWALID() {
	s.Lock()
	defer s.Unlock()

	s.core.nextWalSstID += 1
}

func (s *DBState) refreshDBState(compactorState *CoreDBState) {
	s.Lock()
	defer s.Unlock()

	// copy over L0 up to l0_last_compacted
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

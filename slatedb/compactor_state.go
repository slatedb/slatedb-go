package slatedb

import (
	"github.com/kapetan-io/tackle/set"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/compacted"
	"github.com/slatedb/slatedb-go/slatedb/state"
	"log/slog"
	"math"
	"strconv"

	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

// ------------------------------------------------
// SourceID
// ------------------------------------------------

type SourceIDType int

const (
	SortedRunID SourceIDType = iota + 1
	SSTID
)

type SourceID struct {
	typ   SourceIDType
	value string
}

func newSourceIDSST(id ulid.ULID) SourceID {
	return SourceID{
		typ:   SSTID,
		value: id.String(),
	}
}

func (s SourceID) sortedRunID() mo.Option[uint32] {
	if s.typ != SortedRunID {
		return mo.None[uint32]()
	}
	val, err := strconv.Atoi(s.value)
	if err != nil {
		return mo.None[uint32]()
	}
	return mo.Some(uint32(val))
}

func (s SourceID) sstID() mo.Option[ulid.ULID] {
	if s.typ != SSTID {
		return mo.None[ulid.ULID]()
	}
	val, err := ulid.Parse(s.value)
	if err != nil {
		return mo.None[ulid.ULID]()
	}
	return mo.Some(val)
}

// ------------------------------------------------
// Compaction
// ------------------------------------------------

type CompactionStatus int

const (
	Submitted CompactionStatus = iota + 1
	InProgress
)

type Compaction struct {
	status      CompactionStatus
	sources     []SourceID
	destination uint32
}

func newCompaction(sources []SourceID, destination uint32) Compaction {
	return Compaction{
		status:      Submitted,
		sources:     sources,
		destination: destination,
	}
}

// ------------------------------------------------
// CompactorState
// ------------------------------------------------

type CompactorState struct {
	dbState     *state.CoreStateSnapshot
	compactions map[uint32]Compaction
	log         *slog.Logger
}

func newCompactorState(dbState *state.CoreStateSnapshot, log *slog.Logger) *CompactorState {
	set.Default(&log, slog.Default())

	return &CompactorState{
		compactions: map[uint32]Compaction{},
		dbState:     dbState,
		log:         log,
	}
}

func (c *CompactorState) submitCompaction(compaction Compaction) error {
	_, ok := c.compactions[compaction.destination]
	if ok {
		// we already have an ongoing compaction for this destination
		return common.ErrInvalidCompaction
	}

	for _, sr := range c.dbState.Compacted {
		if sr.ID == compaction.destination {
			if !c.oneOfTheSourceSRMatchesDestination(compaction) {
				// the compaction overwrites an existing sr but doesn't include the sr
				return common.ErrInvalidCompaction
			}
			break
		}
	}

	c.log.Info("accepted submitted compaction:", "compaction", compaction)
	c.compactions[compaction.destination] = compaction
	return nil
}

func (c *CompactorState) oneOfTheSourceSRMatchesDestination(compaction Compaction) bool {
	for _, src := range compaction.sources {
		if src.typ == SortedRunID {
			srcVal, _ := strconv.Atoi(src.value)
			if uint32(srcVal) == compaction.destination {
				return true
			}
		}
	}
	return false
}

func (c *CompactorState) refreshDBState(writerState *state.CoreStateSnapshot) {
	// the writer may have added more l0 SSTs. Add these to our l0 list.
	lastCompactedL0 := c.dbState.L0LastCompacted
	mergedL0s := make([]sstable.Handle, 0)

	for _, writerL0SST := range writerState.L0 {
		assert.True(writerL0SST.Id.Type == sstable.Compacted, "unexpected sstable.ID type")
		writerL0ID, _ := writerL0SST.Id.CompactedID().Get()
		// we stop appending to our l0 list if we encounter sstID equal to lastCompactedID
		lastCompactedL0ID, _ := lastCompactedL0.Get()
		if lastCompactedL0.IsPresent() && writerL0ID == lastCompactedL0ID {
			break
		}
		mergedL0s = append(mergedL0s, writerL0SST)
	}

	merged := c.dbState.Clone()
	merged.L0 = mergedL0s
	merged.LastCompactedWalSSTID.Store(writerState.LastCompactedWalSSTID.Load())
	merged.NextWalSstID.Store(writerState.NextWalSstID.Load())
	c.dbState = merged
}

// update dbState by removing L0 SSTs and compacted SortedRuns that are present
// in Compaction.sources
func (c *CompactorState) finishCompaction(outputSR *compacted.SortedRun) {
	compaction, ok := c.compactions[outputSR.ID]
	if !ok {
		return
	}
	c.log.Info("finished compaction", "compaction", compaction)

	compactionL0s := make(map[ulid.ULID]bool)
	compactionSRs := make(map[uint32]bool)
	for _, srcId := range compaction.sources {
		if srcId.sstID().IsPresent() {
			id, _ := srcId.sstID().Get()
			compactionL0s[id] = true
		} else if srcId.sortedRunID().IsPresent() {
			id, _ := srcId.sortedRunID().Get()
			compactionSRs[id] = true
		}
	}
	compactionSRs[compaction.destination] = true

	dbState := c.dbState.Clone()
	newL0 := make([]sstable.Handle, 0)
	for _, sst := range dbState.L0 {
		assert.True(sst.Id.CompactedID().IsPresent(), "Expected compactedID not present")
		l0ID, _ := sst.Id.CompactedID().Get()
		_, ok := compactionL0s[l0ID]
		if !ok {
			newL0 = append(newL0, sst)
		}
	}

	newCompacted := make([]compacted.SortedRun, 0)
	inserted := false
	for _, sr := range dbState.Compacted {
		if !inserted && outputSR.ID >= sr.ID {
			newCompacted = append(newCompacted, *outputSR)
			inserted = true
		}
		_, ok := compactionSRs[sr.ID]
		if !ok {
			newCompacted = append(newCompacted, sr)
		}
	}
	if !inserted {
		newCompacted = append(newCompacted, *outputSR)
	}

	c.assertCompactedSRsInIDOrder(newCompacted)
	assert.True(len(compaction.sources) > 0, "compaction should not be empty")

	firstSource := compaction.sources[0]
	if firstSource.sstID().IsPresent() {
		compactedL0, _ := firstSource.sstID().Get()
		dbState.L0LastCompacted = mo.Some(compactedL0)
	}

	dbState.L0 = newL0
	dbState.Compacted = newCompacted
	c.dbState = dbState
	delete(c.compactions, outputSR.ID)
}

// sortedRun list should have IDs in decreasing order
func (c *CompactorState) assertCompactedSRsInIDOrder(compacted []compacted.SortedRun) {
	lastSortedRunID := uint32(math.MaxUint32)
	for _, sr := range compacted {
		assert.True(sr.ID < lastSortedRunID, "compacted sortedRuns not in decreasing order")
		lastSortedRunID = sr.ID
	}
}

package slatedb

import (
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"maps"
	"math"
	"slices"
	"strconv"
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

func newSourceIDSortedRun(id uint32) SourceID {
	return SourceID{
		typ:   SortedRunID,
		value: strconv.Itoa(int(id)),
	}
}

func newSourceIDSST(id ulid.ULID) SourceID {
	return SourceID{
		typ:   SSTID,
		value: id.String(),
	}
}

func (s SourceID) sortedRun() mo.Option[uint32] {
	if s.typ != SortedRunID {
		return mo.None[uint32]()
	}
	val, err := strconv.Atoi(s.value)
	if err != nil {
		return mo.None[uint32]()
	}
	return mo.Some(uint32(val))
}

func (s SourceID) sst() mo.Option[ulid.ULID] {
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

func newCompaction(sources []SourceID, destination uint32) *Compaction {
	return &Compaction{
		status:      Submitted,
		sources:     sources,
		destination: destination,
	}
}

// ------------------------------------------------
// CompactorState
// ------------------------------------------------

type CompactorState struct {
	dbState     *CoreDBState
	compactions map[uint32]Compaction
}

func newCompactorState(dbState *CoreDBState) *CompactorState {
	return &CompactorState{
		dbState:     dbState,
		compactions: map[uint32]Compaction{},
	}
}

func (c *CompactorState) getCompactions() []Compaction {
	return slices.Collect(maps.Values(c.compactions))
}

func (c *CompactorState) submitCompaction(compaction Compaction) error {
	_, ok := c.compactions[compaction.destination]
	if ok {
		// we already have an ongoing compaction for this destination
		return common.ErrInvalidCompaction
	}

	for _, sr := range c.dbState.compacted {
		if sr.id == compaction.destination {
			if !c.oneOfTheSourceSRMatchesDestination(compaction) {
				// the compaction overwrites an existing sr but doesn't include the sr
				return common.ErrInvalidCompaction
			}
			break
		}
	}

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

func (c *CompactorState) refreshDBState(writerState *CoreDBState) {
	// the writer may have added more l0 SSTs. Add these to our l0 list.
	lastCompacted, _ := c.dbState.l0LastCompacted.Get()
	mergedL0s := make([]SSTableHandle, 0)

	for _, writerL0SST := range writerState.l0 {
		common.AssertTrue(writerL0SST.id.typ == Compacted, "unexpected SSTableID type")
		writerL0ID, _ := writerL0SST.id.compactedID().Get()
		// we stop appending to our l0 list if we encounter sstID equal to lastCompactedID
		if c.dbState.l0LastCompacted.IsPresent() && writerL0ID == lastCompacted {
			break
		}
		mergedL0s = append(mergedL0s, writerL0SST)
	}

	c.dbState.l0 = mergedL0s
	c.dbState.lastCompactedWalSSTID = writerState.lastCompactedWalSSTID
	c.dbState.nextWalSstID = writerState.nextWalSstID
}

func (c *CompactorState) finishCompaction(outputSR SortedRun) {
	compaction, ok := c.compactions[outputSR.id]
	if !ok {
		return
	}

	compactionL0s := make(map[ulid.ULID]bool)
	compactionSRs := make(map[uint32]bool)
	for _, srcId := range compaction.sources {
		if srcId.sst().IsPresent() {
			id, _ := srcId.sst().Get()
			compactionL0s[id] = true
		} else if srcId.sortedRun().IsPresent() {
			id, _ := srcId.sortedRun().Get()
			compactionSRs[id] = true
		}
	}
	compactionSRs[compaction.destination] = true

	newL0 := make([]SSTableHandle, 0)
	for _, sst := range c.dbState.l0 {
		common.AssertTrue(sst.id.compactedID().IsPresent(), "Expected compactedID not present")
		l0ID, _ := sst.id.compactedID().Get()
		_, ok := compactionL0s[l0ID]
		if !ok {
			newL0 = append(newL0, sst)
		}
	}

	newCompacted := make([]SortedRun, 0)
	inserted := false
	for _, sr := range c.dbState.compacted {
		if !inserted && outputSR.id >= sr.id {
			newCompacted = append(newCompacted, outputSR)
			inserted = true
		}
		_, ok := compactionSRs[sr.id]
		if !ok {
			newCompacted = append(newCompacted, sr)
		}
	}
	if !inserted {
		newCompacted = append(newCompacted, outputSR)
	}

	c.assertCompactedSRsInIDOrder(newCompacted)
	common.AssertTrue(len(compaction.sources) > 0, "compaction should not be empty")

	firstSource := compaction.sources[0]
	if firstSource.sst().IsPresent() {
		compactedL0, _ := firstSource.sst().Get()
		c.dbState.l0LastCompacted = mo.Some(compactedL0)
	}

	c.dbState.l0 = newL0
	c.dbState.compacted = newCompacted
	delete(c.compactions, outputSR.id)
}

// sortedRun list should have IDs in decreasing order
func (c *CompactorState) assertCompactedSRsInIDOrder(compacted []SortedRun) {
	lastSortedRunID := uint32(math.MaxUint32)
	for _, sr := range compacted {
		common.AssertTrue(sr.id < lastSortedRunID, "compacted sortedRuns not in decreasing order")
		lastSortedRunID = sr.id
	}
}

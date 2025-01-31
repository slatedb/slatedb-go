package compaction

import (
	"log/slog"
	"math"
	"strconv"

	"github.com/kapetan-io/tackle/set"

	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/compacted"
	"github.com/slatedb/slatedb-go/slatedb/state"

	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"

	"github.com/slatedb/slatedb-go/slatedb/common"
)

// ------------------------------------------------
// CompactorState
// ------------------------------------------------

type CompactorState struct {
	DbState     *state.CoreStateSnapshot
	Compactions map[uint32]Compaction
	log         *slog.Logger
}

func NewCompactorState(dbState *state.CoreStateSnapshot, log *slog.Logger) *CompactorState {
	set.Default(&log, slog.Default())

	return &CompactorState{
		Compactions: map[uint32]Compaction{},
		DbState:     dbState,
		log:         log,
	}
}

func (c *CompactorState) SubmitCompaction(compaction Compaction) error {
	_, ok := c.Compactions[compaction.destination]
	if ok {
		// we already have an ongoing compaction for this destination
		return common.ErrInvalidCompaction
	}

	for _, sr := range c.DbState.Compacted {
		if sr.ID == compaction.destination {
			if !c.oneOfTheSourceSRMatchesDestination(compaction) {
				// the compaction overwrites an existing sr but doesn't include the sr
				return common.ErrInvalidCompaction
			}
			break
		}
	}

	c.log.Info("accepted submitted compaction:", "compaction", compaction)
	c.Compactions[compaction.destination] = compaction
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

func (c *CompactorState) RefreshDBState(writerState *state.CoreStateSnapshot) {
	// the writer may have added more l0 SSTs. Add these to our l0 list.
	lastCompactedL0 := c.DbState.L0LastCompacted
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

	merged := c.DbState.Clone()
	merged.L0 = mergedL0s
	merged.LastCompactedWalSSTID.Store(writerState.LastCompactedWalSSTID.Load())
	merged.NextWalSstID.Store(writerState.NextWalSstID.Load())
	c.DbState = merged
}

// FinishCompaction update DbState by removing L0 SSTs and compacted SortedRuns that are present
// in Compaction.sources
func (c *CompactorState) FinishCompaction(outputSR *compacted.SortedRun) {
	compaction, ok := c.Compactions[outputSR.ID]
	if !ok {
		return
	}
	c.log.Info("finished compaction", "compaction", compaction)

	compactionL0s := make(map[ulid.ULID]bool)
	compactionSRs := make(map[uint32]bool)
	for _, srcId := range compaction.sources {
		if srcId.SstID().IsPresent() {
			id, _ := srcId.SstID().Get()
			compactionL0s[id] = true
		} else if srcId.SortedRunID().IsPresent() {
			id, _ := srcId.SortedRunID().Get()
			compactionSRs[id] = true
		}
	}
	compactionSRs[compaction.destination] = true

	dbState := c.DbState.Clone()
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
	if firstSource.SstID().IsPresent() {
		compactedL0, _ := firstSource.SstID().Get()
		dbState.L0LastCompacted = mo.Some(compactedL0)
	}

	dbState.L0 = newL0
	dbState.Compacted = newCompacted
	c.DbState = dbState
	delete(c.Compactions, outputSR.ID)
}

// sortedRun list should have IDs in decreasing order
func (c *CompactorState) assertCompactedSRsInIDOrder(compacted []compacted.SortedRun) {
	lastSortedRunID := uint32(math.MaxUint32)
	for _, sr := range compacted {
		assert.True(sr.ID < lastSortedRunID, "compacted sortedRuns not in decreasing order")
		lastSortedRunID = sr.ID
	}
}

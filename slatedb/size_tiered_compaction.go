package slatedb

import (
	"github.com/slatedb/slatedb-go/internal/assert"
)

type SizeTieredCompactionScheduler struct{}

func (s SizeTieredCompactionScheduler) maybeScheduleCompaction(state *CompactorState) []Compaction {
	dbState := state.dbState
	// for now, just compact l0 down to a new sorted run each time
	compactions := make([]Compaction, 0)
	if len(dbState.L0) >= 4 {
		sources := make([]SourceID, 0)
		for _, sst := range dbState.L0 {
			id, ok := sst.Id.CompactedID().Get()
			assert.True(ok, "Expected valid compacted ID")
			sources = append(sources, newSourceIDSST(id))
		}

		nextSortedRunID := uint32(0)
		if len(dbState.Compacted) > 0 {
			nextSortedRunID = dbState.Compacted[0].ID + 1
		}

		compactions = append(compactions, newCompaction(sources, nextSortedRunID))
	}
	return compactions
}

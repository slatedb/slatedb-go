package slatedb

import (
	"github.com/slatedb/slatedb-go/internal/assert"
)

type SizeTieredCompactionScheduler struct{}

func (s SizeTieredCompactionScheduler) maybeScheduleCompaction(state *CompactorState) []Compaction {
	dbState := state.dbState
	// for now, just compact l0 down to a new sorted run each time
	compactions := make([]Compaction, 0)
	if len(dbState.l0) >= 4 {
		sources := make([]SourceID, 0)
		for _, sst := range dbState.l0 {
			id, ok := sst.Id.CompactedID().Get()
			assert.True(ok, "Expected valid compacted ID")
			sources = append(sources, newSourceIDSST(id))
		}

		nextSortedRunID := uint32(0)
		if len(dbState.compacted) > 0 {
			nextSortedRunID = dbState.compacted[0].id + 1
		}

		compactions = append(compactions, newCompaction(sources, nextSortedRunID))
	}
	return compactions
}

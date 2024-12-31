package state

import (
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/stretchr/testify/assert"
	"testing"
)

func addL0sToDBState(dbState *DBState, n uint32) {
	sstInfo := &sstable.Info{
		IndexOffset:      0,
		IndexLen:         0,
		FilterOffset:     0,
		FilterLen:        0,
		CompressionCodec: compress.CodecNone,
	}

	for i := 0; i < int(n); i++ {
		dbState.FreezeMemtable(uint64(i))
		immMemtable := dbState.OldestImmMemtable()
		if immMemtable.IsAbsent() {
			break
		}
		sst := sstable.NewHandle(sstable.NewIDCompacted(ulid.Make()), sstInfo)
		dbState.MoveImmMemtableToL0(immMemtable.MustGet(), sst)
	}
}

func TestRefreshDBStateWithL0sUptoLastCompacted(t *testing.T) {
	dbState := NewDBState(NewCoreDBState())
	addL0sToDBState(dbState, 4)

	// prepare compactorState indicating that the last SST in L0 gets compacted
	compactorState := dbState.CoreStateSnapshot()
	size := len(compactorState.L0)
	lastCompacted := compactorState.L0[size-1]
	compactorState.L0 = compactorState.L0[:size-1]
	assert.Equal(t, sstable.Compacted, lastCompacted.Id.Type)

	id, err := ulid.Parse(lastCompacted.Id.Value)
	assert.NoError(t, err)
	compactorState.L0LastCompacted = mo.Some(id)

	// when refreshDBState is called with the compactorState
	dbState.RefreshDBState(compactorState)

	// then verify that the dbState.core is modified to match the given compactorState
	assert.Equal(t, len(compactorState.L0), len(dbState.L0()))
	for i := 0; i < len(compactorState.L0); i++ {
		expected := compactorState.L0[i]
		actual := dbState.L0()[i]
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, compactorState.L0LastCompacted, dbState.L0LastCompacted())
}

func TestRefreshDBStateWithAllL0sIfNoneCompacted(t *testing.T) {
	dbState := NewDBState(NewCoreDBState())
	addL0sToDBState(dbState, 4)
	l0SSTList := dbState.CoreStateSnapshot().L0

	// when refreshDBState is called with no compaction
	dbState.RefreshDBState(NewCoreDBState().Snapshot())

	// then verify there is no change in dbState L0
	assert.Equal(t, len(l0SSTList), len(dbState.L0()))
	for i := 0; i < len(l0SSTList); i++ {
		expected := l0SSTList[i]
		actual := dbState.L0()[i]
		assert.Equal(t, expected, actual)
	}
}

package slatedb

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
		dbState.freezeMemtable(uint64(i))
		immMemtable := dbState.oldestImmMemtable()
		if immMemtable.IsAbsent() {
			break
		}
		sst := sstable.NewHandle(sstable.NewIDCompacted(ulid.Make()), sstInfo)
		dbState.moveImmMemtableToL0(immMemtable.MustGet(), sst)
	}
}

func TestRefreshDBStateWithL0sUptoLastCompacted(t *testing.T) {
	dbState := newDBState(newCoreDBState())
	addL0sToDBState(dbState, 4)

	// prepare compactorState indicating that the last SST in L0 gets compacted
	compactorState := dbState.coreStateClone()
	size := len(compactorState.l0)
	lastCompacted := compactorState.l0[size-1]
	compactorState.l0 = compactorState.l0[:size-1]
	assert.Equal(t, sstable.Compacted, lastCompacted.Id.Type)

	id, err := ulid.Parse(lastCompacted.Id.Value)
	assert.NoError(t, err)
	compactorState.l0LastCompacted = mo.Some(id)

	// when refreshDBState is called with the compactorState
	dbState.refreshDBState(compactorState)

	// then verify that the dbState.core is modified to match the given compactorState
	assert.Equal(t, len(compactorState.l0), len(dbState.L0()))
	for i := 0; i < len(compactorState.l0); i++ {
		expected := compactorState.l0[i]
		actual := dbState.L0()[i]
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, compactorState.l0LastCompacted, dbState.L0LastCompacted())
}

func TestRefreshDBStateWithAllL0sIfNoneCompacted(t *testing.T) {
	dbState := newDBState(newCoreDBState())
	addL0sToDBState(dbState, 4)
	l0SSTList := dbState.coreStateClone().l0

	// when refreshDBState is called with no compaction
	dbState.refreshDBState(newCoreDBState())

	// then verify there is no change in dbState L0
	assert.Equal(t, len(l0SSTList), len(dbState.L0()))
	for i := 0; i < len(l0SSTList); i++ {
		expected := l0SSTList[i]
		actual := dbState.L0()[i]
		assert.Equal(t, expected, actual)
	}
}

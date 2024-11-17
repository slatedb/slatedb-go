package slatedb

import (
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func addL0sToDBState(dbState *DBState, n uint32) {
	sstInfo := &SSTableInfo{
		firstKey:         mo.None[[]byte](),
		indexOffset:      0,
		indexLen:         0,
		filterOffset:     0,
		filterLen:        0,
		compressionCodec: CompressionNone,
	}

	for i := 0; i < int(n); i++ {
		dbState.freezeMemtable(uint64(i))
		immMemtable := dbState.oldestImmMemtable()
		if immMemtable.IsAbsent() {
			break
		}
		sst := newSSTableHandle(newSSTableIDCompacted(ulid.Make()), sstInfo)
		dbState.moveImmMemtableToL0(immMemtable.MustGet(), sst)
	}
}

func TestRefreshDBStateWithL0sUptoLastCompacted(t *testing.T) {
	dbState := newDBState(newCoreDBState())
	addL0sToDBState(dbState, 4)
	compactorState := dbState.state.core
	size := len(compactorState.l0)
	lastCompacted := compactorState.l0[size-1]
	compactorState.l0 = compactorState.l0[:size-1]
	assert.Equal(t, Compacted, lastCompacted.id.typ)

	id, err := ulid.Parse(lastCompacted.id.value)
	assert.NoError(t, err)
	compactorState.l0LastCompacted = mo.Some(id)

	dbState.refreshDBState(compactorState)

	for i := 0; i < len(compactorState.l0); i++ {
		expected := compactorState.l0[i]
		actual := dbState.state.core.l0[i]
		assert.Equal(t, expected, actual)
	}
}

func TestRefreshDBStateWithAllL0sIfNoneCompacted(t *testing.T) {
	dbState := newDBState(newCoreDBState())
	addL0sToDBState(dbState, 4)
	l0SSTList := dbState.state.core.l0

	dbState.refreshDBState(newCoreDBState())

	for i := 0; i < len(l0SSTList); i++ {
		expected := l0SSTList[i]
		actual := dbState.state.core.l0[i]
		assert.Equal(t, expected, actual)
	}
}

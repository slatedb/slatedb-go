package slatedb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	compaction2 "github.com/slatedb/slatedb-go/slatedb/compaction"
	"github.com/slatedb/slatedb-go/slatedb/config"
	"github.com/slatedb/slatedb-go/slatedb/state"
	"github.com/slatedb/slatedb-go/slatedb/store"
)

var testPath = "/test/db"

func TestShouldRegisterCompactionAsSubmitted(t *testing.T) {
	_, _, compactorState := buildTestState(t)
	err := compactorState.submitCompaction(buildL0Compaction(compactorState.dbState.L0, 0))
	assert.NoError(t, err)

	assert.Equal(t, 1, len(compactorState.compactions))
	assert.Equal(t, Submitted, compactorState.compactions[0].status)
}

func TestShouldUpdateDBStateWhenCompactionFinished(t *testing.T) {
	_, _, compactorState := buildTestState(t)
	beforeCompaction := compactorState.dbState.Clone()
	compaction := buildL0Compaction(beforeCompaction.L0, 0)
	err := compactorState.submitCompaction(compaction)
	assert.NoError(t, err)

	sr := compaction2.SortedRun{
		ID:      0,
		SSTList: beforeCompaction.L0,
	}
	compactorState.finishCompaction(sr.Clone())

	compactedID, _ := beforeCompaction.L0[0].Id.CompactedID().Get()
	l0LastCompacted, _ := compactorState.dbState.L0LastCompacted.Get()
	assert.Equal(t, compactedID, l0LastCompacted)
	assert.Equal(t, 0, len(compactorState.dbState.L0))
	assert.Equal(t, 1, len(compactorState.dbState.Compacted))
	assert.Equal(t, sr.ID, compactorState.dbState.Compacted[0].ID)
	compactedSR := compactorState.dbState.Compacted[0]
	for i := 0; i < len(sr.SSTList); i++ {
		assert.Equal(t, sr.SSTList[i].Id, compactedSR.SSTList[i].Id)
	}
}

func TestShouldRemoveCompactionWhenCompactionFinished(t *testing.T) {
	_, _, compactorState := buildTestState(t)
	beforeCompaction := compactorState.dbState.Clone()
	compaction := buildL0Compaction(beforeCompaction.L0, 0)
	err := compactorState.submitCompaction(compaction)
	assert.NoError(t, err)

	sr := compaction2.SortedRun{
		ID:      0,
		SSTList: beforeCompaction.L0,
	}
	compactorState.finishCompaction(sr.Clone())

	assert.Equal(t, 0, len(compactorState.compactions))
}

func TestShouldRefreshDBStateCorrectlyWhenNeverCompacted(t *testing.T) {
	bucket, sm, compactorState := buildTestState(t)
	option := config.DefaultDBOptions()
	option.L0SSTSizeBytes = 128
	db, err := OpenWithOptions(context.Background(), testPath, bucket, option)
	assert.NoError(t, err)
	defer db.Close()
	db.Put(repeatedChar('a', 16), repeatedChar('b', 48))
	db.Put(repeatedChar('j', 16), repeatedChar('k', 48))

	writerDBState := waitForManifestWithL0Len(sm, len(compactorState.dbState.L0)+1)

	compactorState.refreshDBState(writerDBState)

	assert.True(t, compactorState.dbState.L0LastCompacted.IsAbsent())
	for i := 0; i < len(writerDBState.L0); i++ {
		assert.Equal(t, writerDBState.L0[i].Id.CompactedID(), compactorState.dbState.L0[i].Id.CompactedID())
	}
}

func TestShouldRefreshDBStateCorrectly(t *testing.T) {
	bucket, sm, compactorState := buildTestState(t)
	originalL0s := compactorState.dbState.Clone().L0
	compactedID, ok := originalL0s[len(originalL0s)-1].Id.CompactedID().Get()
	assert.True(t, ok)
	compaction := newCompaction([]SourceID{newSourceIDSST(compactedID)}, 0)
	err := compactorState.submitCompaction(compaction)
	assert.NoError(t, err)
	compactorState.finishCompaction(&compaction2.SortedRun{
		ID:      0,
		SSTList: []sstable.Handle{originalL0s[len(originalL0s)-1]},
	})

	option := config.DefaultDBOptions()
	option.L0SSTSizeBytes = 128
	db, err := OpenWithOptions(context.Background(), testPath, bucket, option)
	assert.NoError(t, err)
	defer db.Close()
	db.Put(repeatedChar('a', 16), repeatedChar('b', 48))
	db.Put(repeatedChar('j', 16), repeatedChar('k', 48))
	writerDBState := waitForManifestWithL0Len(sm, len(originalL0s)+1)
	dbStateBeforeMerge := compactorState.dbState.Clone()

	compactorState.refreshDBState(writerDBState)

	dbState := compactorState.dbState
	// last sst was removed during compaction
	expectedMergedL0s := originalL0s[:len(originalL0s)-1]
	// new sst got added during db.Put() call above
	expectedMergedL0s = append([]sstable.Handle{writerDBState.L0[0]}, expectedMergedL0s...)
	for i := 0; i < len(expectedMergedL0s); i++ {
		expected, _ := expectedMergedL0s[i].Id.CompactedID().Get()
		actual, _ := dbState.L0[i].Id.CompactedID().Get()
		assert.Equal(t, expected, actual)
	}
	for i := 0; i < len(dbStateBeforeMerge.Compacted); i++ {
		srBefore := dbStateBeforeMerge.Compacted[i]
		srAfter := dbState.Compacted[i]
		assert.Equal(t, srBefore.ID, srAfter.ID)
		for j := 0; j < len(srBefore.SSTList); j++ {
			assert.Equal(t, srBefore.SSTList[j].Id, srAfter.SSTList[j].Id)
		}
	}
	assert.Equal(t, writerDBState.LastCompactedWalSSTID.Load(), dbState.LastCompactedWalSSTID.Load())
	assert.Equal(t, writerDBState.NextWalSstID.Load(), dbState.NextWalSstID.Load())
}

func TestShouldRefreshDBStateCorrectlyWhenAllL0Compacted(t *testing.T) {
	bucket, sm, compactorState := buildTestState(t)
	originalL0s := compactorState.dbState.Clone().L0

	sourceIDs := make([]SourceID, 0)
	for _, sst := range originalL0s {
		id, ok := sst.Id.CompactedID().Get()
		assert.True(t, ok)
		sourceIDs = append(sourceIDs, newSourceIDSST(id))
	}
	compaction := newCompaction(sourceIDs, 0)
	err := compactorState.submitCompaction(compaction)
	assert.NoError(t, err)
	compactorState.finishCompaction(&compaction2.SortedRun{
		ID:      0,
		SSTList: originalL0s,
	})
	assert.Equal(t, 0, len(compactorState.dbState.L0))

	option := config.DefaultDBOptions()
	option.L0SSTSizeBytes = 128
	db, err := OpenWithOptions(context.Background(), testPath, bucket, option)
	assert.NoError(t, err)
	defer db.Close()
	db.Put(repeatedChar('a', 16), repeatedChar('b', 48))
	db.Put(repeatedChar('j', 16), repeatedChar('k', 48))
	writerDBState := waitForManifestWithL0Len(sm, len(originalL0s)+1)

	compactorState.refreshDBState(writerDBState)

	dbState := compactorState.dbState
	assert.Equal(t, 1, len(dbState.L0))
	expectedID, _ := writerDBState.L0[0].Id.CompactedID().Get()
	actualID, _ := dbState.L0[0].Id.CompactedID().Get()
	assert.Equal(t, expectedID, actualID)
}

func waitForManifestWithL0Len(storedManifest store.StoredManifest, size int) *state.CoreStateSnapshot {
	startTime := time.Now()
	for time.Since(startTime) < time.Second*10 {
		dbState, err := storedManifest.Refresh()
		assert2.True(err == nil, "")
		if len(dbState.L0) == size {
			return dbState.Clone()
		}
		time.Sleep(time.Millisecond * 50)
	}
	panic("no manifest found with l0 len")
}

func buildL0Compaction(sstList []sstable.Handle, destination uint32) Compaction {
	sources := make([]SourceID, 0)
	for _, sst := range sstList {
		id, ok := sst.Id.CompactedID().Get()
		assert2.True(ok, "expected compacted SST ID")
		sources = append(sources, newSourceIDSST(id))
	}
	return newCompaction(sources, destination)
}

func buildTestState(t *testing.T) (objstore.Bucket, store.StoredManifest, *CompactorState) {
	t.Helper()

	bucket := objstore.NewInMemBucket()
	option := config.DefaultDBOptions()
	option.L0SSTSizeBytes = 128
	db, err := OpenWithOptions(context.Background(), testPath, bucket, option)
	assert2.True(err == nil, "Could not open db")
	l0Count := 5
	for i := 0; i < l0Count; i++ {
		db.Put(repeatedChar(rune('a'+i), 16), repeatedChar(rune('b'+i), 48))
		db.Put(repeatedChar(rune('j'+i), 16), repeatedChar(rune('k'+i), 48))
	}
	db.Close()

	manifestStore := store.NewManifestStore(testPath, bucket)
	sm, err := store.LoadStoredManifest(manifestStore)
	require.NoError(t, err)
	require.NotNil(t, sm)

	assert.True(t, err == nil, "Could not load stored manifest")
	assert.True(t, sm.IsPresent(), "Could not find stored manifest")
	storedManifest, _ := sm.Get()
	compactorState := newCompactorState(storedManifest.DbState(), nil)
	return bucket, storedManifest, compactorState
}

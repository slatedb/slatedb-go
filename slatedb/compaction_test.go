package slatedb

import (
	"context"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/compacted"
	"github.com/slatedb/slatedb-go/slatedb/compaction"
	"github.com/slatedb/slatedb-go/slatedb/config"
	"github.com/slatedb/slatedb-go/slatedb/state"
	"github.com/slatedb/slatedb-go/slatedb/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

var testPath = "/test/db"

func TestCompactorCompactsL0(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	options := dbOptions(compactorOptions().CompactorOptions)
	_, manifestStore, tableStore, db := buildTestDB(options)
	defer func() { _ = db.Close(ctx) }()
	for i := 0; i < 4; i++ {
		require.NoError(t, db.Put(ctx, repeatedChar(rune('a'+i), 16), repeatedChar(rune('b'+i), 48)))
		require.NoError(t, db.Put(ctx, repeatedChar(rune('j'+i), 16), repeatedChar(rune('k'+i), 48)))
	}

	startTime := time.Now()
	dbState := mo.None[*state.CoreStateSnapshot]()
	for time.Since(startTime) < time.Second*10 {
		sm, err := store.LoadStoredManifest(manifestStore)
		assert.NoError(t, err)
		assert.True(t, sm.IsPresent())
		storedManifest, _ := sm.Get()
		if storedManifest.DbState().L0LastCompacted.IsPresent() {
			dbState = mo.Some(storedManifest.DbState().Clone())
			break
		}
		time.Sleep(time.Millisecond * 50)
	}

	assert.True(t, dbState.IsPresent())
	assert.True(t, dbState.MustGet().L0LastCompacted.IsPresent())
	assert.Equal(t, 1, len(dbState.MustGet().Compacted))

	compactedSSTList := dbState.MustGet().Compacted[0].SSTList
	assert.Equal(t, 1, len(compactedSSTList))

	sst := compactedSSTList[0]
	iter, err := sstable.NewIterator(ctx, &sst, tableStore)
	assert.NoError(t, err)
	for i := 0; i < 4; i++ {
		kv, ok := iter.Next(context.Background())
		assert.True(t, ok)
		assert.Equal(t, repeatedChar(rune('a'+i), 16), kv.Key)
		assert.Equal(t, repeatedChar(rune('b'+i), 48), kv.Value)
	}
	for i := 0; i < 4; i++ {
		kv, ok := iter.Next(context.Background())
		assert.True(t, ok)
		assert.Equal(t, repeatedChar(rune('j'+i), 16), kv.Key)
		assert.Equal(t, repeatedChar(rune('k'+i), 48), kv.Value)
	}

	next, ok := iter.Next(context.Background())
	assert.False(t, ok)
	assert.Equal(t, types.KeyValue{}, next)
}

func TestShouldWriteManifestSafely(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	options := dbOptions(nil)
	bucket, manifestStore, tableStore, db := buildTestDB(options)
	sm, err := store.LoadStoredManifest(manifestStore)
	assert.NoError(t, err)
	assert.True(t, sm.IsPresent())
	storedManifest, _ := sm.Get()
	require.NoError(t, db.Put(ctx, repeatedChar('a', 32), repeatedChar('b', 96)))
	require.NoError(t, db.Close(ctx))

	orchestrator, err := compaction.NewOrchestrator(compactorOptions(), manifestStore, tableStore)
	assert.NoError(t, err)

	l0IDsToCompact := make([]compaction.SourceID, 0)
	for _, sst := range orchestrator.State.DbState.L0 {
		id, ok := sst.Id.CompactedID().Get()
		assert.True(t, ok)
		l0IDsToCompact = append(l0IDsToCompact, compaction.NewSourceIDSST(id))
	}

	db, err = OpenWithOptions(context.Background(), testPath, bucket, options)
	assert.NoError(t, err)
	require.NoError(t, db.Put(ctx, repeatedChar('j', 32), repeatedChar('k', 96)))
	require.NoError(t, db.Close(ctx))

	err = orchestrator.SubmitCompaction(compaction.NewCompaction(l0IDsToCompact, 0))
	assert.NoError(t, err)
	orchestrator.WaitForTasksCompletion()
	msg, ok := orchestrator.NextCompactionResult()
	assert.True(t, ok)
	assert.NotNil(t, msg.SortedRun)
	sr := msg.SortedRun

	err = orchestrator.FinishCompaction(sr)
	assert.NoError(t, err)

	// Key aaa... will be compacted and Key jjj... will be in Level0
	dbState, err := storedManifest.Refresh()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dbState.L0))
	assert.Equal(t, 1, len(dbState.Compacted))

	l0ID, ok := dbState.L0[0].Id.CompactedID().Get()
	assert.True(t, ok)
	compactedSSTIDs := make([]ulid.ULID, 0)
	for _, sst := range dbState.Compacted[0].SSTList {
		id, ok := sst.Id.CompactedID().Get()
		assert.True(t, ok)
		compactedSSTIDs = append(compactedSSTIDs, id)
	}
	assert.False(t, slices.Contains(compactedSSTIDs, l0ID))
	assert.Equal(t, l0IDsToCompact[0].SstID(), dbState.L0LastCompacted)
}

func buildTestDB(options config.DBOptions) (objstore.Bucket, *store.ManifestStore, *store.TableStore, *DB) {
	bucket := objstore.NewInMemBucket()
	db, err := OpenWithOptions(context.Background(), testPath, bucket, options)
	assert2.True(err == nil, "Failed to open test database")
	conf := sstable.DefaultConfig()
	conf.BlockSize = 32
	conf.MinFilterKeys = 10
	conf.Compression = options.CompressionCodec
	manifestStore := store.NewManifestStore(testPath, bucket)
	tableStore := store.NewTableStore(bucket, conf, testPath)
	return bucket, manifestStore, tableStore, db
}

func dbOptions(compactorOptions *config.CompactorOptions) config.DBOptions {
	return config.DBOptions{
		FlushInterval:        100 * time.Millisecond,
		ManifestPollInterval: 100 * time.Millisecond,
		MinFilterKeys:        0,
		L0SSTSizeBytes:       128,
		CompactorOptions:     compactorOptions,
		CompressionCodec:     compress.CodecNone,
	}
}

func compactorOptions() config.DBOptions {
	return config.DBOptions{
		CompactorOptions: &config.CompactorOptions{
			PollInterval: 100 * time.Millisecond,
			MaxSSTSize:   1024 * 1024 * 1024,
		},
		Log: slog.Default(),
	}
}

func TestShouldRegisterCompactionAsSubmitted(t *testing.T) {
	_, _, compactorState := buildTestState(t)
	err := compactorState.SubmitCompaction(buildL0Compaction(compactorState.DbState.L0, 0))
	assert.NoError(t, err)

	assert.Equal(t, 1, len(compactorState.Compactions))
	assert.Equal(t, compaction.Submitted, compactorState.Compactions[0].Status)
}

func TestShouldUpdateDBStateWhenCompactionFinished(t *testing.T) {
	_, _, compactorState := buildTestState(t)
	beforeCompaction := compactorState.DbState.Clone()
	l0Compaction := buildL0Compaction(beforeCompaction.L0, 0)
	err := compactorState.SubmitCompaction(l0Compaction)
	assert.NoError(t, err)

	sr := compacted.SortedRun{
		ID:      0,
		SSTList: beforeCompaction.L0,
	}
	compactorState.FinishCompaction(sr.Clone())

	compactedID, _ := beforeCompaction.L0[0].Id.CompactedID().Get()
	l0LastCompacted, _ := compactorState.DbState.L0LastCompacted.Get()
	assert.Equal(t, compactedID, l0LastCompacted)
	assert.Equal(t, 0, len(compactorState.DbState.L0))
	assert.Equal(t, 1, len(compactorState.DbState.Compacted))
	assert.Equal(t, sr.ID, compactorState.DbState.Compacted[0].ID)
	compactedSR := compactorState.DbState.Compacted[0]
	for i := 0; i < len(sr.SSTList); i++ {
		assert.Equal(t, sr.SSTList[i].Id, compactedSR.SSTList[i].Id)
	}
}

func TestShouldRemoveCompactionWhenCompactionFinished(t *testing.T) {
	_, _, compactorState := buildTestState(t)
	beforeCompaction := compactorState.DbState.Clone()
	l0Compaction := buildL0Compaction(beforeCompaction.L0, 0)
	err := compactorState.SubmitCompaction(l0Compaction)
	assert.NoError(t, err)

	sr := compacted.SortedRun{
		ID:      0,
		SSTList: beforeCompaction.L0,
	}
	compactorState.FinishCompaction(sr.Clone())

	assert.Equal(t, 0, len(compactorState.Compactions))
}

func TestShouldRefreshDBStateCorrectlyWhenNeverCompacted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	bucket, sm, compactorState := buildTestState(t)
	option := config.DefaultDBOptions()
	option.L0SSTSizeBytes = 128
	db, err := OpenWithOptions(context.Background(), testPath, bucket, option)
	assert.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	require.NoError(t, db.Put(ctx, repeatedChar('a', 16), repeatedChar('b', 48)))
	require.NoError(t, db.Put(ctx, repeatedChar('j', 16), repeatedChar('k', 48)))
	writerDBState := waitForManifestWithL0Len(sm, len(compactorState.DbState.L0)+1)

	compactorState.RefreshDBState(writerDBState)

	assert.True(t, compactorState.DbState.L0LastCompacted.IsAbsent())
	for i := 0; i < len(writerDBState.L0); i++ {
		assert.Equal(t, writerDBState.L0[i].Id.CompactedID(), compactorState.DbState.L0[i].Id.CompactedID())
	}
}

func TestShouldRefreshDBStateCorrectly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	bucket, sm, compactorState := buildTestState(t)
	originalL0s := compactorState.DbState.Clone().L0
	compactedID, ok := originalL0s[len(originalL0s)-1].Id.CompactedID().Get()
	assert.True(t, ok)
	newCompaction := compaction.NewCompaction([]compaction.SourceID{compaction.NewSourceIDSST(compactedID)}, 0)
	err := compactorState.SubmitCompaction(newCompaction)
	assert.NoError(t, err)
	compactorState.FinishCompaction(&compacted.SortedRun{
		ID:      0,
		SSTList: []sstable.Handle{originalL0s[len(originalL0s)-1]},
	})

	option := config.DefaultDBOptions()
	option.L0SSTSizeBytes = 128
	db, err := OpenWithOptions(context.Background(), testPath, bucket, option)
	assert.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	require.NoError(t, db.Put(ctx, repeatedChar('a', 16), repeatedChar('b', 48)))
	require.NoError(t, db.Put(ctx, repeatedChar('j', 16), repeatedChar('k', 48)))
	writerDBState := waitForManifestWithL0Len(sm, len(originalL0s)+1)
	dbStateBeforeMerge := compactorState.DbState.Clone()

	compactorState.RefreshDBState(writerDBState)

	dbState := compactorState.DbState
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	bucket, sm, compactorState := buildTestState(t)
	originalL0s := compactorState.DbState.Clone().L0

	sourceIDs := make([]compaction.SourceID, 0)
	for _, sst := range originalL0s {
		id, ok := sst.Id.CompactedID().Get()
		assert.True(t, ok)
		sourceIDs = append(sourceIDs, compaction.NewSourceIDSST(id))
	}
	newCompaction := compaction.NewCompaction(sourceIDs, 0)
	err := compactorState.SubmitCompaction(newCompaction)
	assert.NoError(t, err)
	compactorState.FinishCompaction(&compacted.SortedRun{
		ID:      0,
		SSTList: originalL0s,
	})
	assert.Equal(t, 0, len(compactorState.DbState.L0))

	option := config.DefaultDBOptions()
	option.L0SSTSizeBytes = 128
	db, err := OpenWithOptions(context.Background(), testPath, bucket, option)
	assert.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	require.NoError(t, db.Put(ctx, repeatedChar('a', 16), repeatedChar('b', 48)))
	require.NoError(t, db.Put(ctx, repeatedChar('j', 16), repeatedChar('k', 48)))
	writerDBState := waitForManifestWithL0Len(sm, len(originalL0s)+1)

	compactorState.RefreshDBState(writerDBState)

	dbState := compactorState.DbState
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

func buildL0Compaction(sstList []sstable.Handle, destination uint32) compaction.Compaction {
	sources := make([]compaction.SourceID, 0)
	for _, sst := range sstList {
		id, ok := sst.Id.CompactedID().Get()
		assert2.True(ok, "expected compacted SST ID")
		sources = append(sources, compaction.NewSourceIDSST(id))
	}
	return compaction.NewCompaction(sources, destination)
}

func buildTestState(t *testing.T) (objstore.Bucket, store.StoredManifest, *compaction.CompactorState) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	t.Helper()

	bucket := objstore.NewInMemBucket()
	option := config.DefaultDBOptions()
	option.L0SSTSizeBytes = 128
	db, err := OpenWithOptions(context.Background(), testPath, bucket, option)
	require.NoError(t, err)
	require.NotNil(t, db)

	assert2.True(err == nil, "Could not open db")
	l0Count := 5
	for i := 0; i < l0Count; i++ {
		require.NoError(t, db.Put(ctx, repeatedChar(rune('a'+i), 16), repeatedChar(rune('b'+i), 48)))
		require.NoError(t, db.Put(ctx, repeatedChar(rune('j'+i), 16), repeatedChar(rune('k'+i), 48)))
	}
	require.NoError(t, db.Close(ctx))

	manifestStore := store.NewManifestStore(testPath, bucket)
	sm, err := store.LoadStoredManifest(manifestStore)
	require.NoError(t, err)
	require.NotNil(t, sm)

	assert.True(t, err == nil, "Could not load stored manifest")
	assert.True(t, sm.IsPresent(), "Could not find stored manifest")
	storedManifest, _ := sm.Get()
	compactorState := compaction.NewCompactorState(storedManifest.DbState(), nil)
	return bucket, storedManifest, compactorState
}

package slatedb

import (
	"context"
	assert2 "github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/state"
	"github.com/slatedb/slatedb-go/slatedb/store"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
)

func TestCompactorCompactsL0(t *testing.T) {
	options := dbOptions(compactorOptions().CompactorOptions)
	_, manifestStore, tableStore, db := buildTestDB(options)
	defer db.Close()
	for i := 0; i < 4; i++ {
		db.Put(repeatedChar(rune('a'+i), 16), repeatedChar(rune('b'+i), 48))
		db.Put(repeatedChar(rune('j'+i), 16), repeatedChar(rune('k'+i), 48))
	}

	startTime := time.Now()
	dbState := mo.None[*state.CoreStateSnapshot]()
	for time.Since(startTime) < time.Second*10 {
		sm, err := loadStoredManifest(manifestStore)
		assert.NoError(t, err)
		assert.True(t, sm.IsPresent())
		storedManifest, _ := sm.Get()
		if storedManifest.dbState().L0LastCompacted.IsPresent() {
			dbState = mo.Some(storedManifest.dbState().Clone())
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
	iter, err := sstable.NewIterator(&sst, tableStore)
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
	options := dbOptions(nil)
	bucket, manifestStore, tableStore, db := buildTestDB(options)
	sm, err := loadStoredManifest(manifestStore)
	assert.NoError(t, err)
	assert.True(t, sm.IsPresent())
	storedManifest, _ := sm.Get()
	db.Put(repeatedChar('a', 32), repeatedChar('b', 96))
	err = db.Close()
	assert.NoError(t, err)

	orchestrator, err := newCompactionOrchestrator(compactorOptions(), manifestStore, tableStore)
	assert.NoError(t, err)

	l0IDsToCompact := make([]SourceID, 0)
	for _, sst := range orchestrator.state.dbState.L0 {
		id, ok := sst.Id.CompactedID().Get()
		assert.True(t, ok)
		l0IDsToCompact = append(l0IDsToCompact, newSourceIDSST(id))
	}

	db, err = OpenWithOptions(context.Background(), testPath, bucket, options)
	assert.NoError(t, err)
	db.Put(repeatedChar('j', 32), repeatedChar('k', 96))
	err = db.Close()
	assert.NoError(t, err)

	err = orchestrator.submitCompaction(newCompaction(l0IDsToCompact, 0))
	assert.NoError(t, err)
	orchestrator.executor.waitForTasksToComplete()
	msg, ok := orchestrator.executor.nextCompactionResult()
	assert.True(t, ok)
	assert.NotNil(t, msg.SortedRun)
	sr := msg.SortedRun

	err = orchestrator.finishCompaction(sr)
	assert.NoError(t, err)

	// Key aaa... will be compacted and Key jjj... will be in Level0
	dbState, err := storedManifest.refresh()
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
	assert.Equal(t, l0IDsToCompact[0].sstID(), dbState.L0LastCompacted)
}

func buildTestDB(options DBOptions) (objstore.Bucket, *ManifestStore, *store.TableStore, *DB) {
	bucket := objstore.NewInMemBucket()
	db, err := OpenWithOptions(context.Background(), testPath, bucket, options)
	assert2.True(err == nil, "Failed to open test database")
	conf := sstable.DefaultConfig()
	conf.BlockSize = 32
	conf.MinFilterKeys = 10
	conf.Compression = options.CompressionCodec
	manifestStore := newManifestStore(testPath, bucket)
	tableStore := store.NewTableStore(bucket, conf, testPath)
	return bucket, manifestStore, tableStore, db
}

func dbOptions(compactorOptions *CompactorOptions) DBOptions {
	return DBOptions{
		FlushInterval:        100 * time.Millisecond,
		ManifestPollInterval: 100 * time.Millisecond,
		MinFilterKeys:        0,
		L0SSTSizeBytes:       128,
		CompactorOptions:     compactorOptions,
		CompressionCodec:     compress.CodecNone,
	}
}

func compactorOptions() DBOptions {
	return DBOptions{
		CompactorOptions: &CompactorOptions{
			PollInterval: 100 * time.Millisecond,
			MaxSSTSize:   1024 * 1024 * 1024,
		},
		Log: slog.Default(),
	}
}

package slatedb

import (
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"log/slog"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
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
	dbState := mo.None[*CoreDBState]()
	for time.Since(startTime) < time.Second*10 {
		sm, err := loadStoredManifest(manifestStore)
		assert.NoError(t, err)
		assert.True(t, sm.IsPresent())
		storedManifest, _ := sm.Get()
		state := storedManifest.dbState()
		if state.l0LastCompacted.IsPresent() {
			dbState = mo.Some(state.clone())
			break
		}
		time.Sleep(time.Millisecond * 50)
	}

	assert.True(t, dbState.IsPresent())
	state, _ := dbState.Get()
	assert.True(t, state.l0LastCompacted.IsPresent())
	assert.Equal(t, 1, len(state.compacted))

	compactedSSTList := state.compacted[0].sstList
	assert.Equal(t, 1, len(compactedSSTList))

	sst := compactedSSTList[0]
	iter, err := sstable.NewIterator(&sst, tableStore, 1, 1)
	assert.NoError(t, err)
	for i := 0; i < 4; i++ {
		kv, ok := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, repeatedChar(rune('a'+i), 16), kv.Key)
		assert.Equal(t, repeatedChar(rune('b'+i), 48), kv.Value)
	}
	for i := 0; i < 4; i++ {
		kv, ok := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, repeatedChar(rune('j'+i), 16), kv.Key)
		assert.Equal(t, repeatedChar(rune('k'+i), 48), kv.Value)
	}

	next, ok := iter.Next()
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

	compactorMsgCh := make(chan CompactorMainMsg, math.MaxUint8)
	orchestrator, err := newCompactorOrchestrator(compactorOptions(), manifestStore, tableStore, compactorMsgCh)
	assert.NoError(t, err)

	l0IDsToCompact := make([]SourceID, 0)
	for _, sst := range orchestrator.state.dbState.l0 {
		id, ok := sst.Id.CompactedID().Get()
		assert.True(t, ok)
		l0IDsToCompact = append(l0IDsToCompact, newSourceIDSST(id))
	}

	db, err = OpenWithOptions(testPath, bucket, options)
	assert.NoError(t, err)
	db.Put(repeatedChar('j', 32), repeatedChar('k', 96))
	err = db.Close()
	assert.NoError(t, err)

	err = orchestrator.submitCompaction(newCompaction(l0IDsToCompact, 0))
	assert.NoError(t, err)
	msg := <-orchestrator.workerCh
	assert.NotNil(t, msg.CompactionResult)
	sr := msg.CompactionResult

	err = orchestrator.finishCompaction(sr)
	assert.NoError(t, err)

	// Key aaa... will be compacted and Key jjj... will be in Level0
	dbState, err := storedManifest.refresh()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dbState.l0))
	assert.Equal(t, 1, len(dbState.compacted))

	l0ID, ok := dbState.l0[0].Id.CompactedID().Get()
	assert.True(t, ok)
	compactedSSTIDs := make([]ulid.ULID, 0)
	for _, sst := range dbState.compacted[0].sstList {
		id, ok := sst.Id.CompactedID().Get()
		assert.True(t, ok)
		compactedSSTIDs = append(compactedSSTIDs, id)
	}
	assert.False(t, slices.Contains(compactedSSTIDs, l0ID))
	assert.Equal(t, l0IDsToCompact[0].sstID(), dbState.l0LastCompacted)
}

func buildTestDB(options DBOptions) (objstore.Bucket, *ManifestStore, *TableStore, *DB) {
	bucket := objstore.NewInMemBucket()
	db, err := OpenWithOptions(testPath, bucket, options)
	common.AssertTrue(err == nil, "Failed to open test database")
	conf := sstable.DefaultConfig()
	conf.BlockSize = 32
	conf.MinFilterKeys = 10
	conf.Compression = options.CompressionCodec
	manifestStore := newManifestStore(testPath, bucket)
	tableStore := NewTableStore(bucket, conf, testPath)
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

package slatedb

import (
	"github.com/naveen246/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPutGetDelete(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	db, err := OpenWithOptions("/tmp/test_kv_store", bucket, testDBOptions(0, 1024))
	assert.NoError(t, err)

	key := []byte("key1")
	value := []byte("value1")
	db.Put(key, value)
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)

	key = []byte("key2")
	value = []byte("value2")
	db.Put(key, value)
	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)

	db.Delete(key)
	val, err = db.Get(key)
	assert.ErrorIs(t, err, common.ErrKeyNotFound)
	db.Close()
}

func TestPutFlushesMemtable(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	dbPath := "/tmp/test_kv_store"
	db, err := OpenWithOptions(dbPath, bucket, testDBOptions(0, 128))
	assert.NoError(t, err)

	manifestStore := newManifestStore(dbPath, bucket)
	stored, err := loadStoredManifest(manifestStore)
	assert.NoError(t, err)
	assert.True(t, stored.IsPresent())

	storedManifest, _ := stored.Get()
	sstFormat := newSSTableFormat(4096, 10, CompressionNone)
	tableStore := newTableStore(bucket, sstFormat, dbPath)

	lastCompacted := uint64(0)
	for i := 0; i < 3; i++ {
		key := []byte(strings.Repeat("a", 16) + strconv.Itoa(i))
		value := []byte(strings.Repeat("b", 50) + strconv.Itoa(i))
		db.Put(key, value)

		key = []byte(strings.Repeat("j", 16) + strconv.Itoa(i))
		value = []byte(strings.Repeat("k", 50) + strconv.Itoa(i))
		db.Put(key, value)

		dbState := waitForManifestCondition(storedManifest, time.Second*30, func(state *CoreDBState) bool {
			return state.lastCompactedWalSSTID > lastCompacted
		})
		assert.Equal(t, uint64(i*2+2), dbState.lastCompactedWalSSTID)
		lastCompacted = dbState.lastCompactedWalSSTID
	}

	dbState, err := storedManifest.refresh()
	assert.NoError(t, err)
	l0 := dbState.l0
	assert.Equal(t, 3, len(l0))
	for i := 0; i < 3; i++ {
		sst := l0[2-i]
		iter := newSSTIterator(&sst, tableStore, 1, 1)

		kvOption, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, kvOption.IsPresent())
		kv, _ := kvOption.Get()
		key := []byte(strings.Repeat("a", 16) + strconv.Itoa(i))
		value := []byte(strings.Repeat("b", 50) + strconv.Itoa(i))
		assert.Equal(t, key, kv.Key)
		assert.Equal(t, value, kv.Value)

		kvOption, err = iter.Next()
		assert.NoError(t, err)
		assert.True(t, kvOption.IsPresent())
		kv, _ = kvOption.Get()
		key = []byte(strings.Repeat("j", 16) + strconv.Itoa(i))
		value = []byte(strings.Repeat("k", 50) + strconv.Itoa(i))
		assert.Equal(t, key, kv.Key)
		assert.Equal(t, value, kv.Value)

		kvOption, err = iter.Next()
		assert.NoError(t, err)
		assert.False(t, kvOption.IsPresent())
	}
}

func waitForManifestCondition(
	sm StoredManifest,
	timeout time.Duration,
	cond func(state *CoreDBState) bool,
) *CoreDBState {
	start := time.Now()
	for time.Since(start) < timeout {
		dbState, err := sm.refresh()
		common.AssertTrue(err == nil, "")
		if cond(dbState) {
			return dbState.clone()
		}
		time.Sleep(time.Millisecond * 10)
	}
	panic("manifest condition took longer than timeout")
}

func testDBOptions(minFilterKeys uint32, l0SSTSizeBytes uint64) DBOptions {
	return DBOptions{
		FlushMS:              100,
		ManifestPollInterval: time.Duration(100),
		MinFilterKeys:        minFilterKeys,
		L0SSTSizeBytes:       l0SSTSizeBytes,
		CompressionCodec:     CompressionNone,
	}
}

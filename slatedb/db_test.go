package slatedb

import (
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TODO: Look into injecting failpoints
//  https://pkg.go.dev/github.com/pingcap/failpoint ?

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
	err = db.FlushWAL()
	assert.NoError(t, err)
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
		key := repeatedChar(rune('a'+i), 16)
		value := repeatedChar(rune('b'+i), 50)
		db.Put(key, value)

		key = repeatedChar(rune('j'+i), 16)
		value = repeatedChar(rune('k'+i), 50)
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
		key := repeatedChar(rune('a'+i), 16)
		value := repeatedChar(rune('b'+i), 50)
		assert.Equal(t, key, kv.Key)
		assert.Equal(t, value, kv.Value)

		kvOption, err = iter.Next()
		assert.NoError(t, err)
		assert.True(t, kvOption.IsPresent())
		kv, _ = kvOption.Get()
		key = repeatedChar(rune('j'+i), 16)
		value = repeatedChar(rune('k'+i), 50)
		assert.Equal(t, key, kv.Key)
		assert.Equal(t, value, kv.Value)

		kvOption, err = iter.Next()
		assert.NoError(t, err)
		assert.False(t, kvOption.IsPresent())
	}
}

func TestPutEmptyValue(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	db, err := OpenWithOptions("/tmp/test_kv_store", bucket, testDBOptions(0, 1024))
	assert.NoError(t, err)

	key := []byte("key1")
	value := []byte("")
	db.Put(key, value)
	err = db.FlushWAL()
	assert.NoError(t, err)
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)
}

func TestFlushWhileIterating(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	db, err := OpenWithOptions("/tmp/test_kv_store", bucket, testDBOptions(0, 1024))
	assert.NoError(t, err)

	wal := db.state.wal
	wal.put([]byte("abc1111"), []byte("value1111"))
	wal.put([]byte("abc2222"), []byte("value2222"))
	wal.put([]byte("abc3333"), []byte("value3333"))
	memtable := wal.table.clone()

	iter := memtable.iter()

	next, err := iter.Next()
	assert.NoError(t, err)
	kv, _ := next.Get()
	assert.Equal(t, []byte("abc1111"), kv.Key)
	assert.Equal(t, []byte("value1111"), kv.Value)

	err = db.FlushWAL()
	assert.NoError(t, err)

	next, err = iter.Next()
	assert.NoError(t, err)
	kv, _ = next.Get()
	assert.Equal(t, []byte("abc2222"), kv.Key)
	assert.Equal(t, []byte("value2222"), kv.Value)

	next, err = iter.Next()
	assert.NoError(t, err)
	kv, _ = next.Get()
	assert.Equal(t, []byte("abc3333"), kv.Key)
	assert.Equal(t, []byte("value3333"), kv.Value)
}

func TestBasicRestore(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	dbPath := "/tmp/test_kv_store"
	db, err := OpenWithOptions(dbPath, bucket, testDBOptions(0, 128))
	assert.NoError(t, err)

	// do a few writes that will result in l0 flushes
	l0Count := 3
	for i := 0; i < l0Count; i++ {
		key := repeatedChar(rune('a'+i), 16)
		value := repeatedChar(rune('b'+i), 48)
		db.Put(key, value)
		key = repeatedChar(rune('j'+i), 16)
		value = repeatedChar(rune('k'+i), 48)
		db.Put(key, value)
	}

	// write some smaller keys so that we populate wal without flushing to l0
	sstCount := 5
	for i := 0; i < sstCount; i++ {
		db.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		err := db.FlushWAL()
		assert.NoError(t, err)
	}
	db.Close()

	// recover and validate that sst files are loaded on recovery.
	dbRestored, err := OpenWithOptions(dbPath, bucket, testDBOptions(0, 128))
	assert.NoError(t, err)

	for i := 0; i < l0Count; i++ {
		val, err := dbRestored.Get(repeatedChar(rune('a'+i), 16))
		assert.NoError(t, err)
		assert.Equal(t, repeatedChar(rune('b'+i), 48), val)
		val, err = dbRestored.Get(repeatedChar(rune('j'+i), 16))
		assert.NoError(t, err)
		assert.Equal(t, repeatedChar(rune('k'+i), 48), val)
	}
	for i := 0; i < sstCount; i++ {
		val, err := dbRestored.Get([]byte(strconv.Itoa(i)))
		assert.NoError(t, err)
		assert.Equal(t, []byte(strconv.Itoa(i)), val)
	}

	manifestStore := newManifestStore(dbPath, bucket)
	stored, err := loadStoredManifest(manifestStore)
	assert.NoError(t, err)
	assert.True(t, stored.IsPresent())

	storedManifest, _ := stored.Get()
	dbState := storedManifest.dbState()
	assert.Equal(t, uint64(sstCount+2*l0Count+1), dbState.nextWalSstID)
}

func TestShouldReadUncommittedIfReadLevelUncommitted(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	dbPath := "/tmp/test_kv_store"
	db, err := OpenWithOptions(dbPath, bucket, testDBOptions(0, 1024))
	assert.NoError(t, err)

	// we do not wait till WAL is flushed to object store and memtable
	db.PutWithOptions([]byte("foo"), []byte("bar"), WriteOptions{AwaitFlush: false})

	value, err := db.GetWithOptions([]byte("foo"), ReadOptions{ReadLevel: Uncommitted})
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), value)
	db.Close()
}

func TestShouldReadOnlyCommittedData(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	dbPath := "/tmp/test_kv_store"
	db, err := OpenWithOptions(dbPath, bucket, testDBOptions(0, 1024))
	assert.NoError(t, err)

	db.Put([]byte("foo"), []byte("bar"))
	db.PutWithOptions([]byte("foo"), []byte("bla"), WriteOptions{AwaitFlush: false})

	value, err := db.Get([]byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), value)

	value, err = db.GetWithOptions([]byte("foo"), ReadOptions{ReadLevel: Uncommitted})
	assert.NoError(t, err)
	assert.Equal(t, []byte("bla"), value)
	db.Close()
}

func TestShouldDeleteWithoutAwaitingFlush(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	dbPath := "/tmp/test_kv_store"
	db, err := OpenWithOptions(dbPath, bucket, testDBOptions(0, 1024))
	assert.NoError(t, err)

	db.Put([]byte("foo"), []byte("bar"))
	db.DeleteWithOptions([]byte("foo"), WriteOptions{AwaitFlush: false})

	value, err := db.Get([]byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), value)

	value, err = db.GetWithOptions([]byte("foo"), ReadOptions{ReadLevel: Uncommitted})
	assert.ErrorIs(t, err, common.ErrKeyNotFound)
	db.Close()
}

func TestSnapshotState(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	dbPath := "/tmp/test_kv_store"
	db, err := OpenWithOptions(dbPath, bucket, testDBOptions(0, 128))
	assert.NoError(t, err)

	// write a few keys that will result in memtable flushes
	key1 := repeatedChar('a', 32)
	value1 := repeatedChar('b', 96)
	db.Put(key1, value1)
	key2 := repeatedChar('c', 32)
	value2 := repeatedChar('d', 96)
	db.Put(key2, value2)

	db.Close()

	db, err = OpenWithOptions(dbPath, bucket, testDBOptions(0, 128))
	assert.NoError(t, err)
	snapshot := db.state.snapshot()
	assert.Equal(t, uint64(2), snapshot.state.core.lastCompactedWalSSTID)
	assert.Equal(t, uint64(3), snapshot.state.core.nextWalSstID)
	assert.Equal(t, 2, len(snapshot.state.core.l0))
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

func repeatedChar(ch rune, count int) []byte {
	return []byte(strings.Repeat(string(ch), count))
}

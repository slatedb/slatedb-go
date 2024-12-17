package slatedb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/kapetan-io/tackle/set"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"log/slog"
	"math"
	"sync"

	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/thanos-io/objstore"
)

const BlockSize = 4096

type DB struct {
	manifest   *FenceableManifest
	tableStore *TableStore
	compactor  *Compactor
	opts       DBOptions
	state      *DBState

	// walFlushNotifierCh - When DB.Close is called, we send a notification to this channel
	// and the goroutine running the walFlush task reads this channel and shuts down
	walFlushNotifierCh chan bool

	// memtableFlushNotifierCh - When DB.Close is called, we send a Shutdown notification to this channel
	// and the goroutine running the memtableFlush task reads this channel and shuts down
	memtableFlushNotifierCh chan<- MemtableFlushThreadMsg

	// walFlushTaskWG - When DB.Close is called, this is used to wait till the walFlush task goroutine is completed
	walFlushTaskWG *sync.WaitGroup

	// memtableFlushTaskWG - When DB.Close is called, this is used to wait till the memtableFlush task goroutine is completed
	memtableFlushTaskWG *sync.WaitGroup
}

func Open(path string, bucket objstore.Bucket) (*DB, error) {
	return OpenWithOptions(path, bucket, DefaultDBOptions())
}

func OpenWithOptions(path string, bucket objstore.Bucket, options DBOptions) (*DB, error) {
	conf := sstable.DefaultConfig()
	conf.BlockSize = BlockSize
	conf.MinFilterKeys = options.MinFilterKeys
	conf.Compression = options.CompressionCodec
	set.Default(&options.Log, slog.Default())

	tableStore := NewTableStore(bucket, conf, path)
	manifestStore := newManifestStore(path, bucket)
	manifest, err := getManifest(manifestStore)

	if err != nil {
		return nil, err
	}

	memtableFlushNotifierCh := make(chan MemtableFlushThreadMsg, math.MaxUint8)
	state, err := manifest.dbState()
	if err != nil {
		return nil, err
	}

	db, err := newDB(options, tableStore, state, memtableFlushNotifierCh)
	if err != nil {
		return nil, fmt.Errorf("during db init: %w", err)
	}
	db.manifest = manifest

	db.walFlushNotifierCh = make(chan bool, math.MaxUint8)
	// we start 2 background threads
	// one thread for flushing WAL to object store and then to memtable. Flushing happens every FlushInterval Duration
	db.spawnWALFlushTask(db.walFlushNotifierCh, db.walFlushTaskWG)
	// another thread for
	// 1. flushing Immutable memtables to L0. Flushing happens when memtable size reaches L0SSTSizeBytes
	// 2. loading manifest from object store and update current DBState. This happens every ManifestPollInterval milliseconds
	db.spawnMemtableFlushTask(manifest, memtableFlushNotifierCh, db.memtableFlushTaskWG)

	var compactor *Compactor
	if db.opts.CompactorOptions != nil {
		compactor, err = newCompactor(manifestStore, tableStore, db.opts)
		if err != nil {
			return nil, fmt.Errorf("while creating compactor: %w", err)
		}
	}
	db.compactor = compactor

	return db, nil
}

func (db *DB) Close() error {
	if db.compactor != nil {
		db.compactor.close()
	}

	// notify flush task goroutine to shutdown and wait for it to shutdown cleanly
	db.walFlushNotifierCh <- true
	db.walFlushTaskWG.Wait()

	// notify memTable flush task goroutine to shutdown and wait for it to shutdown cleanly
	db.memtableFlushNotifierCh <- Shutdown
	db.memtableFlushTaskWG.Wait()

	return nil
}

func (db *DB) Put(key []byte, value []byte) {
	db.PutWithOptions(key, value, DefaultWriteOptions())
}

func (db *DB) PutWithOptions(key []byte, value []byte, options WriteOptions) {
	assert.True(len(key) > 0, "key cannot be empty")

	currentWAL := db.state.PutKVToWAL(key, value)
	if options.AwaitFlush {
		// we wait for WAL to be flushed to memtable and then we send a notification
		// to goroutine to flush memtable to L0. we do not wait till its flushed to L0
		// because client can read the key from memtable
		currentWAL.Table().AwaitWALFlush()
	}
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.GetWithOptions(key, DefaultReadOptions())
}

// GetWithOptions -
// if readlevel is Uncommitted we start searching key in the following order
// mutable WAL, immutableWALs, mutable memtable, immutable memtables, SSTs in L0, compacted Sorted runs
//
// if readlevel is Committed we start searching key in the following order
// mutable memtable, immutable memtables, SSTs in L0, compacted Sorted runs
func (db *DB) GetWithOptions(key []byte, options ReadOptions) ([]byte, error) {
	snapshot := db.state.snapshot()

	if options.ReadLevel == Uncommitted {
		// search for key in mutable WAL
		val, ok := snapshot.wal.Get(key).Get()
		if ok { // key is present or tombstoned
			return checkValue(val)
		}
		// search for key in ImmutableWALs
		immWALList := snapshot.immWALs
		for i := 0; i < immWALList.Len(); i++ {
			immWAL := immWALList.At(i)
			val, ok := immWAL.Get(key).Get()
			if ok { // key is present or tombstoned
				return checkValue(val)
			}
		}
	}

	// search for key in mutable memtable
	val, ok := snapshot.memtable.Get(key).Get()
	if ok { // key is present or tombstoned
		return checkValue(val)
	}
	// search for key in Immutable memtables
	immMemtables := snapshot.immMemtables
	for i := 0; i < immMemtables.Len(); i++ {
		immTable := immMemtables.At(i)
		val, ok := immTable.Get(key).Get()
		if ok {
			return checkValue(val)
		}
	}

	// search for key in SSTs in L0
	for _, sst := range snapshot.core.l0 {
		if db.sstMayIncludeKey(sst, key) {
			iter, err := sstable.NewIteratorAtKey(&sst, key, db.tableStore.Clone(), 1, 1)
			if err != nil {
				return nil, err
			}

			kv, ok := iter.NextEntry()
			if !ok {
				return nil, err
			}

			if ok && bytes.Equal(kv.Key, key) {
				return checkValue(kv.Value)
			}
		}
	}

	// search for key in compacted Sorted runs
	for _, sr := range snapshot.core.compacted {
		if db.srMayIncludeKey(sr, key) {
			iter, err := newSortedRunIteratorFromKey(sr, key, db.tableStore.Clone(), 1, 1)
			if err != nil {
				return nil, err
			}

			kv, ok := iter.NextEntry()
			if ok && bytes.Equal(kv.Key, key) {
				return checkValue(kv.Value)
			}
		}
	}

	return nil, common.ErrKeyNotFound
}

func (db *DB) Delete(key []byte) {
	db.DeleteWithOptions(key, DefaultWriteOptions())
}

func (db *DB) DeleteWithOptions(key []byte, options WriteOptions) {
	assert.True(len(key) > 0, "key cannot be empty")

	currentWAL := db.state.DeleteKVFromWAL(key)
	if options.AwaitFlush {
		currentWAL.Table().AwaitWALFlush()
	}
}

func (db *DB) sstMayIncludeKey(sst sstable.Handle, key []byte) bool {
	if !sst.RangeCoversKey(key) {
		return false
	}
	filter, err := db.tableStore.ReadFilter(&sst)
	if err == nil && filter.IsPresent() {
		bFilter, _ := filter.Get()
		return bFilter.HasKey(key)
	}
	return true
}

func (db *DB) srMayIncludeKey(sr SortedRun, key []byte) bool {
	sstOption := sr.sstWithKey(key)
	if sstOption.IsAbsent() {
		return false
	}
	sst, _ := sstOption.Get()
	filter, err := db.tableStore.ReadFilter(&sst)
	if err == nil && filter.IsPresent() {
		bFilter, _ := filter.Get()
		return bFilter.HasKey(key)
	}
	return true
}

// this is to recover from a crash. we read the WALs from object store (considered to be Uncommmitted)
// and write the kv pairs to memtable
func (db *DB) replayWAL() error {
	walIDLastCompacted := db.state.LastCompactedWALID()
	walSSTList, err := db.tableStore.getWalSSTList(walIDLastCompacted)
	if err != nil {
		return err
	}

	lastSSTID := walIDLastCompacted
	for _, sstID := range walSSTList {
		lastSSTID = sstID
		sst, err := db.tableStore.OpenSST(sstable.NewIDWal(sstID))
		if err != nil {
			return err
		}
		assert.True(sst.Id.WalID().IsPresent(), "Invalid WAL ID")

		// iterate through kv pairs in sst and populate walReplayBuf
		iter, err := sstable.NewIterator(sst, db.tableStore.Clone(), 1, 1)
		if err != nil {
			return err
		}

		walReplayBuf := make([]types.RowEntry, 0)
		for {
			kvDel, ok := iter.NextEntry()
			if !ok {
				break
			}
			walReplayBuf = append(walReplayBuf, kvDel)
		}

		// update memtable with kv pairs in walReplayBuf
		for _, kvDel := range walReplayBuf {
			if kvDel.Value.IsTombstone() {
				db.state.DeleteKVFromMemtable(kvDel.Key)
			} else {
				db.state.PutKVToMemtable(kvDel.Key, kvDel.Value.Value)
			}
		}

		db.maybeFreezeMemtable(db.state, sstID)
		if db.state.NextWALID() == sstID {
			db.state.incrementNextWALID()
		}
	}

	assert.True(lastSSTID+1 == db.state.NextWALID(), "")
	return nil
}

func (db *DB) maybeFreezeMemtable(state *DBState, walID uint64) {
	if state.Memtable().Size() < int64(db.opts.L0SSTSizeBytes) {
		return
	}
	state.freezeMemtable(walID)
	db.memtableFlushNotifierCh <- FlushImmutableMemtables
}

// FlushMemtableToL0 - Normally Memtable is flushed to Level0 of object store when it reaches a size of DBOptions.L0SSTSizeBytes
// This method allows the user to flush Memtable to Level0 irrespective of Memtable size.
func (db *DB) FlushMemtableToL0() error {
	lastWalID := db.state.Memtable().LastWalID()
	if lastWalID.IsAbsent() {
		return errors.New("WAL is not yet flushed to Memtable")
	}

	walID, _ := lastWalID.Get()
	db.state.freezeMemtable(walID)

	flusher := MemtableFlusher{
		db:       db,
		manifest: db.manifest,
		log:      db.opts.Log,
	}
	return flusher.flushImmMemtablesToL0()
}

func getManifest(manifestStore *ManifestStore) (*FenceableManifest, error) {
	stored, err := loadStoredManifest(manifestStore)
	if err != nil {
		return nil, err
	}

	var storedManifest *StoredManifest
	sm, ok := stored.Get()
	if ok {
		storedManifest = &sm
	} else {
		storedManifest, err = newStoredManifest(manifestStore, newCoreDBState())
		if err != nil {
			return nil, err
		}
	}

	return initFenceableManifestWriter(storedManifest)
}

func newDB(
	options DBOptions,
	tableStore *TableStore,
	coreDBState *CoreDBState,
	memtableFlushNotifierCh chan<- MemtableFlushThreadMsg,
) (*DB, error) {

	state := newDBState(coreDBState)
	db := &DB{
		state:                   state,
		opts:                    options,
		tableStore:              tableStore,
		memtableFlushNotifierCh: memtableFlushNotifierCh,
		walFlushTaskWG:          &sync.WaitGroup{},
		memtableFlushTaskWG:     &sync.WaitGroup{},
	}
	err := db.replayWAL()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func checkValue(val types.Value) ([]byte, error) {
	if val.GetValue().IsAbsent() { // key is tombstoned/deleted
		return nil, common.ErrKeyNotFound
	} else { // key is present
		value, _ := val.GetValue().Get()
		return value, nil
	}
}

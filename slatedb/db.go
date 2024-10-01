package slatedb

import (
	"bytes"
	"github.com/naveen246/slatedb-go/slatedb/common"
	"github.com/thanos-io/objstore"
	"math"
	"sync"
)

const BlockSize = 4096

type DB struct {
	state      *DBState
	options    DBOptions
	tableStore *TableStore
	compactor  *Compactor

	// walFlushNotifierCh - When DB.Close is called, we send a notification to this channel
	// and the goroutine running the walFlush task reads this channel and shuts down
	walFlushNotifierCh chan bool

	// memtableFlushNotifierCh - When DB.Close is called, we send a Shutdown notification to this channel
	// and the goroutine running the memtableFlush task reads this channel and shuts down
	memtableFlushNotifierCh chan<- MemtableFlushThreadMsg

	// walFlushTaskWG - When DB.Close is called, this is used to wait till the flush task goroutine is completed
	walFlushTaskWG *sync.WaitGroup

	// memtableFlushTaskWG - When DB.Close is called, this is used to wait till the memtableFlush task goroutine is completed
	memtableFlushTaskWG *sync.WaitGroup
}

func Open(path string, bucket objstore.Bucket) (*DB, error) {
	return OpenWithOptions(path, bucket, DefaultDBOptions())
}

func OpenWithOptions(path string, bucket objstore.Bucket, options DBOptions) (*DB, error) {
	sstFormat := newSSTableFormat(BlockSize, options.MinFilterKeys, options.CompressionCodec)
	tableStore := newTableStore(bucket, sstFormat, path)
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
		return nil, err
	}

	db.walFlushNotifierCh = make(chan bool, math.MaxUint8)
	// we start 2 background threads
	// one thread for flushing WAL to object store and then to memtable. Flushing happens every FlushMS seconds
	db.spawnWALFlushTask(db.walFlushNotifierCh, db.walFlushTaskWG)
	// another thread for
	// 1. flushing Immutable memtables to L0. Flushing happens when memtable size reaches L0SSTSizeBytes
	// 2. loading manifest from object store and update current DBState. This happens every ManifestPollInterval milliseconds
	db.spawnMemtableFlushTask(manifest, memtableFlushNotifierCh, db.memtableFlushTaskWG)

	var compactor *Compactor
	if db.options.CompactorOptions != nil {
		compactor, err = newCompactor(manifestStore, tableStore, db.options.CompactorOptions)
		if err != nil {
			return nil, err
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
	common.AssertTrue(len(key) > 0, "key cannot be empty")

	currentWAL := db.state.wal
	currentWAL.put(key, value)
	if options.AwaitFlush {
		// we wait for WAL to be flushed to memtable and then we send a notification
		// to goroutine to flush memtable to L0. we do not wait till its flushed to L0
		// because client can read the key from memtable
		<-currentWAL.table.awaitWALFlush()
	}
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.GetWithOptions(key, DefaultReadOptions())
}

// GetWithOptions -
// if readlevel is Uncommitted we start searching key in the following order
// active WAL, immutableWALs, active memtable, immutable memtables, SSTs in L0, compacted Sorted runs
//
// if readlevel is Committed we start searching key in the following order
// active memtable, immutable memtables, SSTs in L0, compacted Sorted runs
func (db *DB) GetWithOptions(key []byte, options ReadOptions) ([]byte, error) {
	snapshot := db.state.snapshot()

	if options.ReadLevel == Uncommitted {
		// search for key in active WAL
		val, ok := snapshot.wal.get(key).Get()
		if ok { // key is present or tombstoned
			return checkValue(val)
		}
		// search for key in ImmutableWALs
		immWALList := snapshot.state.immWAL
		for i := 0; i < immWALList.Len(); i++ {
			table := immWALList.At(i).table
			val, ok := table.get(key).Get()
			if ok { // key is present or tombstoned
				return checkValue(val)
			}
		}
	}

	// search for key in active memtable
	val, ok := snapshot.memtable.get(key).Get()
	if ok { // key is present or tombstoned
		return checkValue(val)
	}
	// search for key in Immutable memtables
	immMemtables := snapshot.state.immMemtable
	for i := 0; i < immMemtables.Len(); i++ {
		table := immMemtables.At(i).table
		val, ok := table.get(key).Get()
		if ok {
			return checkValue(val)
		}
	}

	// search for key in SSTs in L0
	for _, sst := range snapshot.state.core.l0 {
		if db.sstMayIncludeKey(sst, key) {
			iter := newSSTIteratorFromKey(&sst, key, db.tableStore.clone(), 1, 1)
			entry, err := iter.NextEntry()
			if err != nil {
				return nil, err
			}
			kv, ok := entry.Get()
			if ok && bytes.Equal(kv.Key, key) {
				return checkValue(kv.ValueDel)
			}
		}
	}

	// search for key in compacted Sorted runs
	for _, sr := range snapshot.state.core.compacted {
		if db.srMayIncludeKey(sr, key) {
			iter := newSortedRunIteratorFromKey(sr, key, db.tableStore.clone(), 1, 1)
			entry, err := iter.NextEntry()
			if err != nil {
				return nil, err
			}
			kv, ok := entry.Get()
			if ok && bytes.Equal(kv.Key, key) {
				return checkValue(kv.ValueDel)
			}
		}
	}

	return nil, common.ErrKeyNotFound
}

func (db *DB) Delete(key []byte) {
	db.DeleteWithOptions(key, DefaultWriteOptions())
}

func (db *DB) DeleteWithOptions(key []byte, options WriteOptions) {
	common.AssertTrue(len(key) > 0, "key cannot be empty")

	currentWAL := db.state.wal
	currentWAL.delete(key)
	if options.AwaitFlush {
		<-currentWAL.table.awaitWALFlush()
	}
}

func (db *DB) sstMayIncludeKey(sst SSTableHandle, key []byte) bool {
	if !sst.rangeCoversKey(key) {
		return false
	}
	filter, err := db.tableStore.readFilter(&sst)
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
	filter, err := db.tableStore.readFilter(&sst)
	if err == nil && filter.IsPresent() {
		bFilter, _ := filter.Get()
		return bFilter.HasKey(key)
	}
	return true
}

// this is to recover from a crash. we read the WALs from object store (considered to be Uncommmitted)
// and write the kv pairs to memtable
func (db *DB) replayWAL() error {
	walIDLastCompacted := db.state.getState().core.lastCompactedWalSSTID
	walSSTList, err := db.tableStore.getWalSSTList(walIDLastCompacted)
	if err != nil {
		return err
	}

	lastSSTID := walIDLastCompacted
	for _, sstID := range walSSTList {
		lastSSTID = sstID
		sst, err := db.tableStore.openSST(newSSTableIDWal(sstID))
		if err != nil {
			return err
		}
		common.AssertTrue(sst.id.walID().IsPresent(), "Invalid WAL ID")

		// iterate through kv pairs in sst and populate walReplayBuf
		iter := newSSTIterator(sst, db.tableStore.clone(), 1, 1)
		walReplayBuf := make([]common.KVDeletable, 0)
		for {
			entry, err := iter.NextEntry()
			if err != nil {
				return err
			}
			kvDel, _ := entry.Get()
			if entry.IsAbsent() {
				break
			}
			walReplayBuf = append(walReplayBuf, kvDel)
		}

		// update memtable with kv pairs in walReplayBuf
		for _, kvDel := range walReplayBuf {
			if kvDel.ValueDel.IsTombstone {
				db.state.memtable.delete(kvDel.Key)
			} else {
				db.state.memtable.put(kvDel.Key, kvDel.ValueDel.Value)
			}
		}

		db.maybeFreezeMemtable(db.state, sstID)
		if db.state.state.core.nextWalSstID == sstID {
			db.state.incrementNextWALID()
		}
	}

	common.AssertTrue(lastSSTID+1 == db.state.getState().core.nextWalSstID, "")
	return nil
}

func (db *DB) maybeFreezeMemtable(state *DBState, walID uint64) {
	if state.memtable.size.Load() < int64(db.options.L0SSTSizeBytes) {
		return
	}
	state.freezeMemtable(walID)
	db.memtableFlushNotifierCh <- FlushImmutableMemtables
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
		options:                 options,
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

func checkValue(val common.ValueDeletable) ([]byte, error) {
	if val.GetValue() == nil { // key is tombstoned/deleted
		return nil, common.ErrKeyNotFound
	} else { // key is present
		return val.GetValue(), nil
	}
}

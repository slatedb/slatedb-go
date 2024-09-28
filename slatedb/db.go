package slatedb

import (
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

	// flushNotifierCh - When DB.Close is called, we send a notification to this channel
	// and the goroutine that is running the flush task reads this channel and stops running
	flushNotifierCh chan bool

	// memtableFlushNotifierCh - When DB.Close is called, we send a Shutdown notification to this channel
	// and the goroutine that is running the memtableFlush task reads this channel and stops running
	memtableFlushNotifierCh chan<- MemtableFlushThreadMsg

	// flushTaskWG - When DB.Close is called, this is used to wait till the flush task is completed
	flushTaskWG *sync.WaitGroup

	// memtableFlushTaskWG - When DB.Close is called, this is used to wait till the memtableFlush task is completed
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

	db.flushNotifierCh = make(chan bool, math.MaxUint8)
	db.spawnFlushTask(db.flushNotifierCh, db.flushTaskWG)
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
	db.flushNotifierCh <- true
	db.flushTaskWG.Wait()

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
		<-currentWAL.table.awaitFlush()
	}
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.GetWithOptions(key, DefaultReadOptions())
}

func (db *DB) GetWithOptions(key []byte, options ReadOptions) ([]byte, error) {
	// TODO: implement
	return nil, nil
}

func (db *DB) Delete(key []byte) {
	db.DeleteWithOptions(key, DefaultWriteOptions())
}

func (db *DB) DeleteWithOptions(key []byte, options WriteOptions) {
	common.AssertTrue(len(key) > 0, "key cannot be empty")

	currentWAL := db.state.wal
	currentWAL.delete(key)
	if options.AwaitFlush {
		<-currentWAL.table.awaitFlush()
	}
}

func (db *DB) replayWAL() error {
	// TODO: implement
	return nil
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
	}
	err := db.replayWAL()
	if err != nil {
		return nil, err
	}
	return db, nil
}

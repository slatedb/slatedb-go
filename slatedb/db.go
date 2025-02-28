package slatedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/kapetan-io/tackle/set"
	"github.com/slatedb/slatedb-go/internal"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/compacted"
	"github.com/slatedb/slatedb-go/slatedb/compaction"
	"github.com/slatedb/slatedb-go/slatedb/config"
	"github.com/slatedb/slatedb-go/slatedb/state"
	"github.com/slatedb/slatedb-go/slatedb/store"
	"github.com/thanos-io/objstore"
)

const BlockSize = 4096

// ErrInternal indicates an internal error outside the control of slatedb
// has occurred. For instance, if the `DBOptions.on_corruption` callback
// was invoked and returned an error.
//
// This error indicates a fatal error has occurred, as such, any value
// returned along with this error should be considered inconsistent.
type ErrInternal = internal.ExportedInternalError

// ErrRetryable indicates a Transient error has occurred. For instance
// an IO or network timeout has occurred. This error indicates the
// request was unable to complete, and should be retried.
type ErrRetryable = internal.ExportedRetryableError

// ErrInvalidArgument indicates the request contained invalid parameters.
// For instance, if a `Get()` was called with an empty or nil key.
//
// This error indicates the request cannot be completed as requested, the
// caller must modify one or more arguments of the call for the request to
// complete.
type ErrInvalidArgument = internal.ExportedInvalidArgument

// ErrKeyNotFound indicates the requested key was not found in the
// database.
var ErrKeyNotFound = errors.New("key not found")

// TODO(thrawn01): Export the Corruption Types here

type DB struct {
	manifest   *store.FenceableManifest
	tableStore *store.TableStore
	compactor  *compaction.Compactor
	opts       config.DBOptions
	state      *state.DBState

	// walFlushNotifierCh - When DB.Close is called, we send a notification to this channel
	// and the goroutine running the walFlush task reads this channel and shuts down
	walFlushNotifierCh chan context.Context

	// memtableFlushNotifierCh - When DB.Close is called, we send a Shutdown notification to this channel
	// and the goroutine running the memtableFlush task reads this channel and shuts down
	memtableFlushNotifierCh chan<- MemtableFlushThreadMsg

	// walFlushTaskWG - When DB.Close is called, this is used to wait till the walFlush task goroutine is completed
	walFlushTaskWG *sync.WaitGroup

	// memtableFlushTaskWG - When DB.Close is called, this is used to wait till the memtableFlush task goroutine is completed
	memtableFlushTaskWG *sync.WaitGroup
}

func Open(ctx context.Context, path string, bucket objstore.Bucket) (*DB, error) {
	return OpenWithOptions(ctx, path, bucket, config.DefaultDBOptions())
}

func OpenWithOptions(ctx context.Context, path string, bucket objstore.Bucket, options config.DBOptions) (*DB, error) {
	conf := sstable.DefaultConfig()
	conf.BlockSize = BlockSize
	conf.MinFilterKeys = options.MinFilterKeys
	conf.Compression = options.CompressionCodec
	set.Default(&options.Log, slog.Default())

	tableStore := store.NewTableStore(bucket, conf, path)
	manifestStore := store.NewManifestStore(path, bucket)
	manifest, err := getManifest(manifestStore)

	if err != nil {
		return nil, err
	}

	memtableFlushNotifierCh := make(chan MemtableFlushThreadMsg, math.MaxUint8)
	dbState, err := manifest.DbState()
	if err != nil {
		return nil, err
	}

	db, err := newDB(ctx, options, tableStore, dbState.ToCoreState(), memtableFlushNotifierCh)
	if err != nil {
		return nil, fmt.Errorf("during db init: %w", err)
	}
	db.manifest = manifest

	db.walFlushNotifierCh = make(chan context.Context, math.MaxUint8)
	// we start 2 background threads
	// one thread for flushing WAL to object store and then to memtable. Flushing happens every FlushInterval Duration
	db.spawnWALFlushTask(db.walFlushNotifierCh, db.walFlushTaskWG)
	// another thread for
	// 1. flushing Immutable memtables to L0. Flushing happens when memtable size reaches L0SSTSizeBytes
	// 2. loading manifest from object store and update current DBState. This happens every ManifestPollInterval milliseconds
	db.spawnMemtableFlushTask(manifest, memtableFlushNotifierCh, db.memtableFlushTaskWG)

	var compactor *compaction.Compactor
	if db.opts.CompactorOptions != nil {
		compactor, err = compaction.NewCompactor(manifestStore, tableStore, db.opts)
		if err != nil {
			return nil, fmt.Errorf("while creating compactor: %w", err)
		}
	}
	db.compactor = compactor

	return db, nil
}

func (db *DB) Close(ctx context.Context) error {
	var errs []error

	if db.compactor != nil {
		if err := db.compactor.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	// notify flush task goroutine to shutdown and wait for it to shutdown cleanly
	db.walFlushNotifierCh <- ctx
	done := make(chan struct{})
	go func() {
		db.walFlushTaskWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		errs = append(errs, ctx.Err())
	}

	// notify memTable flush task goroutine to shutdown and wait for it to shutdown cleanly
	db.memtableFlushNotifierCh <- Shutdown
	done = make(chan struct{})
	go func() {
		db.memtableFlushTaskWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		errs = append(errs, ctx.Err())
	}

	return errors.Join(errs...)
}

func (db *DB) Put(ctx context.Context, key []byte, value []byte) error {
	return db.PutWithOptions(ctx, key, value, config.DefaultWriteOptions())
}

func (db *DB) PutWithOptions(ctx context.Context, key []byte, value []byte, options config.WriteOptions) error {
	if len(key) == 0 {
		return internal.ErrInvalidArgument("argument 'key' cannot be empty or nil")
	}

	currentWAL := db.state.WalPut(types.RowEntry{
		Value: types.Value{
			Kind:  types.KindKeyValue,
			Value: value,
		},
		Key: key,
	})
	if options.AwaitDurable {
		// we wait for WAL to be flushed to memtable and then we send a notification
		// to goroutine to flush memtable to L0. we do not wait till its flushed to L0
		// because client can read the key from memtable
		return currentWAL.Table().AwaitWALFlush(ctx)
	}
	return nil
}

func (db *DB) Get(ctx context.Context, key []byte) ([]byte, error) {
	return db.GetWithOptions(ctx, key, config.DefaultReadOptions())
}

// GetWithOptions -
// if readlevel is Uncommitted we start searching key in the following order
// mutable WAL, immutableWALs, mutable memtable, immutable memtables, SSTs in L0, compacted Sorted runs
//
// if readlevel is Committed we start searching key in the following order
// mutable memtable, immutable memtables, SSTs in L0, compacted Sorted runs
func (db *DB) GetWithOptions(ctx context.Context, key []byte, options config.ReadOptions) ([]byte, error) {
	snapshot := db.state.Snapshot()

	if options.ReadLevel == config.Uncommitted {
		// search for key in mutable WAL
		val, ok := snapshot.Wal.Get(key).Get()
		if ok { // key is present or tombstoned
			return checkValue(val)
		}
		// search for key in ImmutableWALs
		immWALList := snapshot.ImmWALs
		for i := 0; i < immWALList.Len(); i++ {
			immWAL := immWALList.At(i)
			val, ok := immWAL.Get(key).Get()
			if ok { // key is present or tombstoned
				return checkValue(val)
			}
		}
	}

	// search for key in mutable memtable
	val, ok := snapshot.Memtable.Get(key).Get()
	if ok { // key is present or tombstoned
		return checkValue(val)
	}
	// search for key in Immutable memtables
	immMemtables := snapshot.ImmMemtables
	for i := 0; i < immMemtables.Len(); i++ {
		immTable := immMemtables.At(i)
		val, ok := immTable.Get(key).Get()
		if ok {
			return checkValue(val)
		}
	}

	// search for key in SSTs in L0
	for _, sst := range snapshot.Core.L0 {
		if db.sstMayIncludeKey(ctx, sst, key) {
			iter, err := sstable.NewIteratorAtKey(ctx, &sst, key, db.tableStore.Clone())
			if err != nil {
				return nil, err
			}

			kv, ok := iter.Next(ctx)
			if ok && bytes.Equal(kv.Key, key) {
				return checkValue(kv.Value)
			}
		}
	}

	// search for key in compacted Sorted runs
	for _, sr := range snapshot.Core.Compacted {
		if db.srMayIncludeKey(ctx, sr, key) {
			iter, err := compacted.NewSortedRunIteratorFromKey(ctx, sr, key, db.tableStore.Clone())
			if err != nil {
				return nil, err
			}

			kv, ok := iter.Next(ctx)
			if ok && bytes.Equal(kv.Key, key) {
				return checkValue(kv.Value)
			}
		}
	}

	return nil, ErrKeyNotFound
}

func (db *DB) Delete(ctx context.Context, key []byte) error {
	return db.DeleteWithOptions(ctx, key, config.DefaultWriteOptions())
}

func (db *DB) DeleteWithOptions(ctx context.Context, key []byte, options config.WriteOptions) error {
	if len(key) == 0 {
		return internal.ErrInvalidArgument("argument 'key' cannot be empty or nil")
	}

	currentWAL := db.state.WalPut(types.RowEntry{
		Value: types.Value{
			Kind: types.KindTombStone,
		},
		Key: key,
	})
	if options.AwaitDurable {
		return currentWAL.Table().AwaitWALFlush(ctx)
	}
	return nil
}

func (db *DB) sstMayIncludeKey(ctx context.Context, sst sstable.Handle, key []byte) bool {
	if !sst.RangeCoversKey(key) {
		return false
	}
	filter, err := db.tableStore.ReadFilter(ctx, &sst)
	if err == nil && filter.IsPresent() {
		bFilter, _ := filter.Get()
		return bFilter.HasKey(key)
	}
	return true
}

func (db *DB) srMayIncludeKey(ctx context.Context, sr compacted.SortedRun, key []byte) bool {
	sstOption := sr.SstWithKey(key)
	if sstOption.IsAbsent() {
		return false
	}
	sst, _ := sstOption.Get()
	filter, err := db.tableStore.ReadFilter(ctx, &sst)
	if err == nil && filter.IsPresent() {
		bFilter, _ := filter.Get()
		return bFilter.HasKey(key)
	}
	return true
}

// this is to recover from a crash. we read the WALs from object store (considered to be Uncommmitted)
// and write the kv pairs to memtable
func (db *DB) replayWAL(ctx context.Context) error {
	walIDLastCompacted := db.state.LastCompactedWALID()
	walSSTList, err := db.tableStore.GetWalSSTList(walIDLastCompacted)
	if err != nil {
		return err
	}

	lastSSTID := walIDLastCompacted
	for _, sstID := range walSSTList {
		lastSSTID = sstID
		sst, err := db.tableStore.OpenSST(ctx, sstable.NewIDWal(sstID))
		if err != nil {
			return err
		}
		assert.True(sst.Id.WalID().IsPresent(), "Invalid WAL ID")

		// iterate through kv pairs in sst and populate walReplayBuf
		iter, err := sstable.NewIterator(ctx, sst, db.tableStore.Clone())
		if err != nil {
			return err
		}

		walReplayBuf := make([]types.RowEntry, 0)
		for {
			kvDel, ok := iter.Next(ctx)
			if !ok {
				break
			}
			walReplayBuf = append(walReplayBuf, kvDel)
		}

		// update memtable with kv pairs in walReplayBuf
		for _, entry := range walReplayBuf {
			db.state.MemTablePut(entry)
		}

		db.maybeFreezeMemtable(db.state, sstID)
		if db.state.NextWALID() == sstID {
			db.state.IncrementNextWALID()
		}
	}

	assert.True(lastSSTID+1 == db.state.NextWALID(), "")
	return nil
}

func (db *DB) maybeFreezeMemtable(dbState *state.DBState, walID uint64) {
	if dbState.Memtable().Size() < int64(db.opts.L0SSTSizeBytes) {
		return
	}
	dbState.FreezeMemtable(walID)
	db.memtableFlushNotifierCh <- FlushImmutableMemtables
}

// FlushMemtableToL0 - Normally Memtable is flushed to Level0 of object store when it reaches a size of DBOptions.L0SSTSizeBytes
// This method allows the user to flush Memtable to Level0 irrespective of Memtable size.
func (db *DB) FlushMemtableToL0() error {
	lastWalID := db.state.Memtable().LastWalID()
	if lastWalID.IsAbsent() {
		return internal.Err("assertion failed; WAL is not yet flushed to Memtable")
	}

	walID, _ := lastWalID.Get()
	db.state.FreezeMemtable(walID)

	flusher := MemtableFlusher{
		db:       db,
		manifest: db.manifest,
		log:      db.opts.Log,
	}
	return flusher.flushImmMemtablesToL0()
}

func getManifest(manifestStore *store.ManifestStore) (*store.FenceableManifest, error) {
	stored, err := store.LoadStoredManifest(manifestStore)
	if err != nil {
		return nil, err
	}

	var storedManifest *store.StoredManifest
	sm, ok := stored.Get()
	if ok {
		storedManifest = &sm
	} else {
		storedManifest, err = store.NewStoredManifest(manifestStore, state.NewCoreDBState())
		if err != nil {
			return nil, err
		}
	}

	return store.NewWriterFenceableManifest(storedManifest)
}

func newDB(
	ctx context.Context,
	options config.DBOptions,
	tableStore *store.TableStore,
	coreDBState *state.CoreDBState,
	memtableFlushNotifierCh chan<- MemtableFlushThreadMsg,
) (*DB, error) {

	dbState := state.NewDBState(coreDBState)
	db := &DB{
		state:                   dbState,
		opts:                    options,
		tableStore:              tableStore,
		memtableFlushNotifierCh: memtableFlushNotifierCh,
		walFlushTaskWG:          &sync.WaitGroup{},
		memtableFlushTaskWG:     &sync.WaitGroup{},
	}
	err := db.replayWAL(ctx)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func checkValue(val types.Value) ([]byte, error) {
	if val.GetValue().IsAbsent() { // key is tombstoned/deleted
		return nil, ErrKeyNotFound
	} else { // key is present
		value, _ := val.GetValue().Get()
		return value, nil
	}
}

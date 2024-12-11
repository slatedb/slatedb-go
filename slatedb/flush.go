package slatedb

import (
	"errors"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/table"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"go.uber.org/zap"
)

func (db *DB) spawnWALFlushTask(walFlushNotifierCh <-chan bool, walFlushTaskWG *sync.WaitGroup) {
	walFlushTaskWG.Add(1)
	go func() {
		defer walFlushTaskWG.Done()
		ticker := time.NewTicker(db.options.FlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				db.FlushWAL()
			case <-walFlushNotifierCh:
				db.FlushWAL()
				return
			}
		}
	}()
}

// FlushWAL
// 1. Convert mutable WAL to Immutable WAL
// 2. Flush each Immutable WAL to object store and then to memtable
func (db *DB) FlushWAL() error {
	db.state.freezeWAL()
	err := db.flushImmWALs()
	if err != nil {
		return err
	}
	return nil
}

// For each Immutable WAL
// Flush Immutable WAL to Object store
// Flush Immutable WAL to mutable Memtable
// If memtable has reached size L0SSTBytes then convert memtable to Immutable memtable
// Notify any client(with AwaitFlush set to true) that flush has happened
func (db *DB) flushImmWALs() error {
	for {
		oldestWal := db.state.oldestImmWAL()
		if oldestWal.IsAbsent() {
			break
		}

		immWal := oldestWal.MustGet()
		// Flush Immutable WAL to Object store
		_, err := db.flushImmWAL(immWal)
		if err != nil {
			return err
		}
		db.state.popImmWAL()

		// flush to the memtable before notifying so that data is available for reads
		db.flushImmWALToMemtable(immWal, db.state.Memtable())
		db.maybeFreezeMemtable(db.state, immWal.ID())
		immWal.Table().NotifyWALFlushed()
	}
	return nil
}

func (db *DB) flushImmWAL(immWAL *table.ImmutableWAL) (*sstable.Handle, error) {
	walID := sstable.NewIDWal(immWAL.ID())
	return db.flushImmTable(walID, immWAL.Iter())
}

func (db *DB) flushImmWALToMemtable(immWal *table.ImmutableWAL, memtable *table.Memtable) {
	iter := immWal.Iter()
	for {
		entry, err := iter.NextEntry()
		if err != nil || entry.IsAbsent() {
			break
		}
		kv, _ := entry.Get()
		if kv.Value.IsTombstone() {
			memtable.Delete(kv.Key)
		} else {
			memtable.Put(kv.Key, kv.Value.Value)
		}
	}
	memtable.SetLastWalID(immWal.ID())
}

func (db *DB) flushImmTable(id sstable.ID, iter *table.KVTableIterator) (*sstable.Handle, error) {
	sstBuilder := db.tableStore.TableBuilder()
	for {
		entry, err := iter.NextEntry()
		if err != nil || entry.IsAbsent() {
			break
		}
		kv, _ := entry.Get()
		val := mo.None[[]byte]()
		if !kv.Value.IsTombstone() {
			val = mo.Some(kv.Value.Value)
		}
		err = sstBuilder.Add(kv.Key, val)
		if err != nil {
			return nil, err
		}
	}

	encodedSST, err := sstBuilder.Build()
	if err != nil {
		return nil, err
	}

	sst, err := db.tableStore.WriteSST(id, encodedSST)
	if err != nil {
		return nil, err
	}

	return sst, nil
}

// ------------------------------------------------
// MemtableFlusher
// ------------------------------------------------

func (db *DB) spawnMemtableFlushTask(
	manifest *FenceableManifest,
	memtableFlushNotifierCh <-chan MemtableFlushThreadMsg,
	memtableFlushTaskWG *sync.WaitGroup,
) {
	memtableFlushTaskWG.Add(1)
	isShutdown := false
	go func() {
		defer memtableFlushTaskWG.Done()
		flusher := MemtableFlusher{
			db:       db,
			manifest: manifest,
		}
		ticker := time.NewTicker(db.options.ManifestPollInterval)
		defer ticker.Stop()

		// Stop the loop when the shut down has been received and all
		// remaining memtableFlushNotifierCh channel is drained.
		for !(isShutdown && len(memtableFlushNotifierCh) == 0) {
			select {
			case <-ticker.C:
				err := flusher.loadManifest()
				if err != nil {
					logger.Error("error load manifest", zap.Error(err))
				}
			case val := <-memtableFlushNotifierCh:
				if val == Shutdown {
					isShutdown = true
				} else if val == FlushImmutableMemtables {
					err := flusher.flushImmMemtablesToL0()
					if err != nil {
						logger.Error("Error flushing memtable", zap.Error(err))
					}
				}
			}
		}

		err := flusher.writeManifestSafely()
		if err != nil {
			logger.Error("error writing manifest on shutdown", zap.Error(err))
		}
	}()
}

type MemtableFlushThreadMsg int

const (
	Shutdown MemtableFlushThreadMsg = iota + 1
	FlushImmutableMemtables
)

type MemtableFlusher struct {
	db       *DB
	manifest *FenceableManifest
}

func (m *MemtableFlusher) loadManifest() error {
	currentManifest, err := m.manifest.refresh()
	if err != nil {
		return err
	}
	m.db.state.refreshDBState(currentManifest)
	return nil
}

func (m *MemtableFlusher) writeManifest() error {
	core := m.db.state.coreStateClone()
	return m.manifest.updateDBState(core)
}

func (m *MemtableFlusher) writeManifestSafely() error {
	for {
		err := m.loadManifest()
		if err != nil {
			return err
		}

		err = m.writeManifest()
		if errors.Is(err, common.ErrManifestVersionExists) {
			logger.Warn("conflicting manifest version. retry write", zap.Error(err))
		} else if err != nil {
			return err
		} else {
			return nil
		}
	}
}

func (m *MemtableFlusher) flushImmMemtablesToL0() error {
	for {
		immMemtable := m.db.state.oldestImmMemtable()
		if immMemtable.IsAbsent() {
			break
		}

		id := sstable.NewIDCompacted(ulid.Make())
		sstHandle, err := m.db.flushImmTable(id, immMemtable.MustGet().Iter())
		if err != nil {
			return err
		}

		m.db.state.moveImmMemtableToL0(immMemtable.MustGet(), sstHandle)
		err = m.writeManifestSafely()
		if err != nil {
			return err
		}
	}
	return nil
}

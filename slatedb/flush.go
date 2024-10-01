package slatedb

import (
	"errors"
	"github.com/naveen246/slatedb-go/slatedb/common"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"log"
	"sync"
	"time"
)

func (db *DB) spawnWALFlushTask(walFlushNotifierCh <-chan bool, walFlushTaskWG *sync.WaitGroup) {
	walFlushTaskWG.Add(1)
	go func() {
		defer walFlushTaskWG.Done()
		ticker := time.NewTicker(time.Duration(db.options.FlushMS) * time.Millisecond)
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
//  1. Convert active WAL to Immutable WAL
//  2. For each Immutable WAL
//     Flush Immutable WAL to Object store
//     Flush Immutable WAL to Memtable
//     If memtable has reached size L0SSTBytes then convert memtable to Immutable memtable
//     Notify any client(with AwaitFlush set to true) that flush has happened
func (db *DB) FlushWAL() error {
	db.state.freezeWAL()
	err := db.flushImmWALs()
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) flushImmWALs() error {
	for {
		walList := db.state.getState().immWAL
		if walList == nil || walList.Len() == 0 {
			break
		}
		immWal := walList.Back()
		_, err := db.flushImmWAL(immWal)
		if err != nil {
			return err
		}
		db.state.popImmWAL()

		// flush to the memtable before notifying so that data is available for reads
		db.flushImmWALToMemtable(db.state.memtable, immWal.table)
		db.maybeFreezeMemtable(db.state, immWal.id)
		immWal.table.notifyWALFlushed()
	}
	return nil
}

func (db *DB) flushImmWAL(imm ImmutableWAL) (*SSTableHandle, error) {
	walID := newSSTableIDWal(imm.id)
	return db.flushImmTable(walID, imm.table)
}

func (db *DB) flushImmWALToMemtable(memtable *WritableKVTable, immTable *KVTable) {
	iter := immTable.iter()
	for {
		entry, err := iter.NextEntry()
		if err != nil || entry.IsAbsent() {
			break
		}
		kv, _ := entry.Get()
		if kv.ValueDel.IsTombstone {
			memtable.delete(kv.Key)
		} else {
			memtable.put(kv.Key, kv.ValueDel.Value)
		}
	}
}

func (db *DB) flushImmTable(id SSTableID, immTable *KVTable) (*SSTableHandle, error) {
	sstBuilder := db.tableStore.tableBuilder()
	iter := immTable.iter()
	for {
		entry, err := iter.NextEntry()
		if err != nil || entry.IsAbsent() {
			break
		}
		kv, _ := entry.Get()
		val := mo.None[[]byte]()
		if !kv.ValueDel.IsTombstone {
			val = mo.Some(kv.ValueDel.Value)
		}
		err = sstBuilder.add(kv.Key, val)
		if err != nil {
			return nil, err
		}
	}

	encodedSST, err := sstBuilder.build()
	if err != nil {
		return nil, err
	}

	sst, err := db.tableStore.writeSST(id, encodedSST)
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
		ticker := time.NewTicker(db.options.ManifestPollInterval * time.Millisecond)
		defer ticker.Stop()

		// Stop the loop when the shut down has been received and all
		// remaining memtableFlushNotifierCh channel is drained.
		for !(isShutdown && len(memtableFlushNotifierCh) == 0) {
			select {
			case <-ticker.C:
				err := flusher.loadManifest()
				if err != nil {
					log.Println("Error loading manifest:", err)
				}
			case val := <-memtableFlushNotifierCh:
				if val == Shutdown {
					isShutdown = true
				} else if val == FlushImmutableMemtables {
					err := flusher.flushImmMemtablesToL0()
					if err != nil {
						log.Println("Error flushing memtable:", err)
					}
				}
			}
		}

		err := flusher.writeManifestSafely()
		if err != nil {
			log.Println("Error writing manifest on shutdown:", err)
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
	core := m.db.state.getState().core
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
			log.Println("conflicting manifest version. retry write")
		} else if err != nil {
			return err
		} else {
			return nil
		}
	}
}

func (m *MemtableFlusher) flushImmMemtablesToL0() error {
	for {
		state := m.db.state.getState()
		if state.immMemtable.Len() == 0 {
			break
		}

		immMemtable := state.immMemtable.Back()
		id := newSSTableIDCompacted(ulid.Make())
		sstHandle, err := m.db.flushImmTable(id, immMemtable.table)
		if err != nil {
			return err
		}

		m.db.state.moveImmMemtableToL0(immMemtable, sstHandle)
		err = m.writeManifestSafely()
		if err != nil {
			return err
		}
	}
	return nil
}

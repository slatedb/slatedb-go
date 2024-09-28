package slatedb

import (
	"errors"
	"github.com/naveen246/slatedb-go/slatedb/common"
	"github.com/oklog/ulid/v2"
	"log"
	"sync"
)

func (db *DB) Flush() error {
	db.state.freezeWAL()
	err := db.flushImmWALs()
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) flushImmTable(id SSTableID, immTable *KVTable) (*SSTableHandle, error) {
	// TODO: implement
	return nil, nil
}

func (db *DB) flushImmWAL(imm ImmutableWAL) (*SSTableHandle, error) {
	// TODO: implement
	return nil, nil
}

func (db *DB) flushImmWALToMemtable(memtable *WritableKVTable, immTable *KVTable) {
	// TODO: implement
	return
}

func (db *DB) flushImmWALs() error {
	// TODO: implement
	return nil
}

func (db *DB) spawnFlushTask(flushNotifierCh <-chan bool, flushTaskWG *sync.WaitGroup) {
	flushTaskWG.Add(1)
	go func() {
		defer flushTaskWG.Done()
		for {
			select {
			case <-flushNotifierCh:
				return
			default:
				// Do other stuff
			}
		}
	}()
}

func (db *DB) spawnMemtableFlushTask(
	manifest *FenceableManifest,
	memtableFlushNotifierCh <-chan MemtableFlushThreadMsg,
	memtableFlushTaskWG *sync.WaitGroup,
) {
	memtableFlushTaskWG.Add(1)
	go func() {
		defer memtableFlushTaskWG.Done()
		for {
			select {
			case val := <-memtableFlushNotifierCh:
				if val == Shutdown {
					return
				}
				// do other work
			default:
				// Do other stuff
			}
		}
	}()
}

// ------------------------------------------------
// MemtableFlusher
// ------------------------------------------------

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

package slatedb

import (
	"fmt"
	"github.com/oklog/ulid/v2"
)

func GetState(db *DB) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	wal := db.state.wal.clone()
	walState, err := getKvTableState(wal.table)
	if err != nil {
		return nil, err
	}
	m["wal"] = walState

	memtable := db.state.memtable.clone()
	memtableState, err := getKvTableState(memtable.table)
	if err != nil {
		return nil, err
	}
	m["memtable"] = memtableState

	m["wal_index"] = float64(db.state.state.core.nextWalSstID)

	// TODO: implement immutable_wal and immutable_memtable
	m["immutable_wal"] = make([]interface{}, 0)
	m["immutable_memtable"] = make(map[string]interface{})

	// get the l0 from core state
	l0ssts := db.state.state.core.l0
	l0s := make([]interface{}, len(l0ssts))
	for i, sst := range l0ssts {
		l0s[i] = fmt.Sprintf("compacted/ulid-%04d.sst", sst.id.compactedID().MustGet().Time())
	}
	m["l0"] = l0s
	return m, nil
}

func getKvTableState(table *KVTable) (map[string]interface{}, error) {
	iter := table.iter()
	m := make(map[string]interface{})
	for {
		optionalKv, err := iter.NextEntry()
		if err != nil {
			return nil, err
		}
		if optionalKv.IsAbsent() {
			break
		}
		kv := optionalKv.MustGet()
		//fmt.Printf("Key: %s, Value: %v\n", kv.Key, kv.ValueDel)
		if kv.ValueDel.IsTombstone {
			m[string(kv.Key)] = "notfound"
		} else {
			m[string(kv.Key)] = string(kv.ValueDel.Value)
		}
	}
	return m, nil
}

func WalSstToMap(db *DB, sstId uint64) (map[string]interface{}, error) {
	sst, err := db.tableStore.openSST(newSSTableIDWal(sstId))
	if err != nil {
		return nil, err
	}
	return KeyValSstToMap(db, sst)
}

func CompactedSstToMap(db *DB, sstId ulid.ULID) (map[string]interface{}, error) {
	sst, err := db.tableStore.openSST(newSSTableIDCompacted(sstId))
	if err != nil {
		return nil, err
	}
	return KeyValSstToMap(db, sst)
}

func KeyValSstToMap(db *DB, sst *SSTableHandle) (map[string]interface{}, error) {
	iter, err := newSSTIterator(sst, db.tableStore.clone(), 1, 1)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	for {
		entry, err := iter.NextEntry()
		if err != nil {
			return nil, err
		}
		kvDel, _ := entry.Get()
		if entry.IsAbsent() {
			break
		}
		if string(kvDel.Key) == "placeholder" {
			continue
		}
		if kvDel.ValueDel.IsTombstone {
			m[string(kvDel.Key)] = "notfound"
		} else {
			m[string(kvDel.Key)] = string(kvDel.ValueDel.Value)
		}
	}
	return m, nil
}

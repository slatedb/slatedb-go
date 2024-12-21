package slatedb

import (
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/table"
)

func GetState(db *DB) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	wal := db.state.wal.Clone()
	walState, err := getKvTableState(wal.Iter())
	if err != nil {
		return nil, err
	}
	m["wal"] = walState

	memtable := db.state.memtable.Clone()
	memtableState, err := getKvTableState(memtable.Iter())
	if err != nil {
		return nil, err
	}
	m["memtable"] = memtableState

	m["wal_index"] = float64(db.state.core.nextWalSstID.Load())

	// TODO: implement immutable_wal and immutable_memtable
	m["immutable_wal"] = make([]interface{}, 0)
	m["immutable_memtable"] = make(map[string]interface{})

	// get the l0 from core state
	l0ssts := db.state.core.l0
	l0s := make([]interface{}, len(l0ssts))
	for i, sst := range l0ssts {
		l0s[i] = fmt.Sprintf("compacted/ulid-%04d.sst", sst.Id.CompactedID().MustGet().Time())
	}
	m["l0"] = l0s
	return m, nil
}

func getKvTableState(iter *table.KVTableIterator) (map[string]interface{}, error) {
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
		if kv.Value.IsTombstone() {
			m[string(kv.Key)] = "notfound"
		} else {
			m[string(kv.Key)] = string(kv.Value.Value)
		}
	}
	return m, nil
}

func WalSstToMap(db *DB, sstId uint64) (map[string]interface{}, error) {
	sst, err := db.tableStore.OpenSST(sstable.NewIDWal(sstId))
	if err != nil {
		return nil, err
	}
	return KeyValSstToMap(db, sst)
}

func CompactedSstToMap(db *DB, sstId ulid.ULID) (map[string]interface{}, error) {
	sst, err := db.tableStore.OpenSST(sstable.NewIDCompacted(sstId))
	if err != nil {
		return nil, err
	}
	return KeyValSstToMap(db, sst)
}

func KeyValSstToMap(db *DB, sst *sstable.Handle) (map[string]interface{}, error) {
	iter, err := sstable.NewIterator(sst, db.tableStore.Clone(), 1, 1)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	for {
		entry, ok := iter.NextEntry()
		if !ok {
			break
		}

		if string(entry.Key) == "placeholder" {
			continue
		}
		if entry.Value.IsTombstone() {
			m[string(entry.Key)] = "notfound"
		} else {
			m[string(entry.Key)] = string(entry.Value.Value)
		}
	}
	return m, nil
}

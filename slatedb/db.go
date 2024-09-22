package slatedb

import "github.com/thanos-io/objstore"

type DB struct {
}

func Open(options DBOptions, path string, bucket objstore.Bucket) (DB, error) {
	return DB{}, nil
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) Put(key []byte, value []byte) error {
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (db *DB) Delete(key []byte) error {
	return nil
}

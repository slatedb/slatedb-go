package internal

import (
	"bytes"
	"context"
	"github.com/thanos-io/objstore"
	"io"
	"path"
)

// ------------------------------------------------
// TableStore
// ------------------------------------------------

type TableStore struct {
	bucket        objstore.Bucket
	sstFormat     *SSTableFormat
	rootPath      string
	walPath       string
	compactedPath string
}

func newTableStore(bucket objstore.Bucket, format *SSTableFormat, rootPath string) *TableStore {
	return &TableStore{
		bucket:    bucket,
		sstFormat: format,
		rootPath:  rootPath,
	}
}

func (ts *TableStore) tableBuilder() *EncodedSSTableBuilder {
	return ts.sstFormat.tableBuilder()
}

func (ts *TableStore) writeSST(id SSTableId, encodedSST *EncodedSSTable) (*SSTableHandle, error) {
	dataPath := ts.path(id)
	totalSize := 0
	for _, block := range encodedSST.unconsumedBlocks {
		totalSize += len(block)
	}

	data := make([]byte, 0, totalSize)
	for _, block := range encodedSST.unconsumedBlocks {
		data = append(data, block...)
	}

	err := ts.bucket.Upload(context.Background(), dataPath, bytes.NewReader(data))
	if err != nil {
		return nil, ErrObjectStore
	}

	// TODO: write to cache filter

	return &SSTableHandle{
		id:   id,
		info: encodedSST.sstInfo,
	}, nil
}

func (ts *TableStore) openSST(id SSTableId) (*SSTableHandle, error) {
	obj := ReadOnlyObject{
		bucket: ts.bucket,
		path:   ts.path(id),
	}
	info, err := ts.sstFormat.readInfo(obj)
	if err != nil {
		return nil, err
	}

	return &SSTableHandle{id, info}, nil
}

func (ts *TableStore) path(id SSTableId) string {
	if id.typ == WAL {
		return path.Join(ts.rootPath, ts.walPath, id.value)
	} else if id.typ == Compacted {
		return path.Join(ts.rootPath, ts.compactedPath, id.value)
	}
	return ""
}

// ------------------------------------------------
// ReadOnlyObject
// ------------------------------------------------

type ReadOnlyObject struct {
	bucket objstore.Bucket
	path   string
}

func (r ReadOnlyObject) len() (int, error) {
	attr, err := r.bucket.Attributes(context.Background(), r.path)
	if err != nil {
		return 0, ErrObjectStore
	}
	return int(attr.Size), nil
}

func (r ReadOnlyObject) readRange(rng Range) ([]byte, error) {
	read, err := r.bucket.GetRange(context.Background(), r.path, int64(rng.start), int64(rng.end-rng.start))
	if err != nil {
		return nil, ErrObjectStore
	}

	data, err := io.ReadAll(read)
	if err != nil {
		return nil, ErrObjectStore
	}

	return data, nil
}

func (r ReadOnlyObject) read() ([]byte, error) {
	read, err := r.bucket.Get(context.Background(), r.path)
	if err != nil {
		return nil, ErrObjectStore
	}

	data, err := io.ReadAll(read)
	if err != nil {
		return nil, ErrObjectStore
	}

	return data, nil
}

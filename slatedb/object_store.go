package slatedb

import (
	"bytes"
	"context"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/thanos-io/objstore"
	"io"
	"path"
)

type ObjectStore interface {
	putIfNotExists(path string, data []byte) error

	get(path string) ([]byte, error)

	list(path mo.Option[string]) ([]string, error)
}

type DelegatingObjectStore struct {
	rootPath string
	bucket   objstore.Bucket
}

func newDelegatingObjectStore(rootPath string, bucket objstore.Bucket) *DelegatingObjectStore {
	return &DelegatingObjectStore{rootPath, bucket}
}

func (d *DelegatingObjectStore) getPath(objPath string) string {
	return path.Join(d.rootPath, objPath)
}

func basePath(objPath string) string {
	return path.Base(objPath)
}

// TODO: We should make this atomic
func (d *DelegatingObjectStore) putIfNotExists(objPath string, data []byte) error {
	fullPath := d.getPath(objPath)
	exists, err := d.bucket.Exists(context.Background(), fullPath)
	if err != nil {
		return common.ErrObjectStore
	}

	if exists {
		return common.ErrObjectExists
	}

	err = d.bucket.Upload(context.Background(), fullPath, bytes.NewReader(data))
	if err != nil {
		return common.ErrObjectStore
	}
	return nil
}

func (d *DelegatingObjectStore) get(objPath string) ([]byte, error) {
	fullPath := d.getPath(objPath)
	reader, err := d.bucket.Get(context.Background(), fullPath)
	if err != nil {
		return nil, common.ErrObjectStore
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (d *DelegatingObjectStore) list(objPath mo.Option[string]) ([]string, error) {
	fullPath := d.rootPath
	if objPath.IsPresent() {
		p, _ := objPath.Get()
		fullPath = d.getPath(p)
	}

	objList := make([]string, 0)
	err := d.bucket.Iter(context.Background(), fullPath, func(filepath string) error {
		objList = append(objList, filepath)
		return nil
	}, objstore.WithRecursiveIter)
	if err != nil {
		return nil, common.ErrObjectStore
	}

	return objList, nil
}

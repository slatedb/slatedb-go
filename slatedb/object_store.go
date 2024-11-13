package slatedb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"github.com/thanos-io/objstore"
	"go.uber.org/zap"
)

type ObjectStore interface {
	putIfNotExists(path string, data []byte) error

	get(path string) ([]byte, error)

	list(path mo.Option[string]) ([]ObjectMeta, error)
}

type ObjectMeta struct {
	// LastModified is the timestamp the object was last modified.
	LastModified time.Time

	// Location is the path of the object
	Location string
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
		logger.Error("unable to read data", zap.Error(err))
		return nil, err
	}
	return data, nil
}

func (d *DelegatingObjectStore) list(objPath mo.Option[string]) ([]ObjectMeta, error) {
	fullPath := d.rootPath
	if objPath.IsPresent() {
		p, _ := objPath.Get()
		fullPath = d.getPath(p)
	}

	objMetaList := make([]ObjectMeta, 0)
	iterFn := func(attrs objstore.IterObjectAttributes) error {
		lastModified, _ := attrs.LastModified()
		objMetaList = append(objMetaList, ObjectMeta{
			LastModified: lastModified,
			Location:     attrs.Name,
		})
		return nil
	}
	err := d.bucket.IterWithAttributes(context.Background(), fullPath, iterFn, objstore.WithRecursiveIter())
	if err != nil {
		fmt.Println(err)
		return nil, common.ErrObjectStore
	}

	return objMetaList, nil
}

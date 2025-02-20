package store

import (
	"bytes"
	"context"
	"io"
	"path"
	"slices"
	"time"

	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal"
	"github.com/thanos-io/objstore"
)

type ObjectMeta struct {
	// LastModified is the time the object was last modified.
	LastModified time.Time

	// Location is the path of the object
	Location string
}

type ObjectStore interface {
	putIfNotExists(path string, data []byte) error

	get(path string) ([]byte, error)

	list(path mo.Option[string]) ([]ObjectMeta, error)
}

type DelegatingObjectStore struct {
	rootPath string
	bucket   objstore.Bucket
}

func newDelegatingObjectStore(rootPath string, bucket objstore.Bucket) *DelegatingObjectStore {
	return &DelegatingObjectStore{rootPath, bucket}
}

// TODO: We should make this atomic
func (d *DelegatingObjectStore) putIfNotExists(objPath string, data []byte) error {
	fullPath := path.Join(d.rootPath, objPath)
	exists, err := d.bucket.Exists(context.Background(), fullPath)
	if err != nil {
		return internal.ErrRetryable("during bucket exists check: %s", err)
	}
	if exists {
		return internal.ErrAlreadyExists
	}

	err = d.bucket.Upload(context.Background(), fullPath, bytes.NewReader(data))
	if err != nil {
		return internal.ErrRetryable("during bucket upload: %s", err)
	}
	return nil
}

func (d *DelegatingObjectStore) get(objPath string) ([]byte, error) {
	fullPath := path.Join(d.rootPath, objPath)
	reader, err := d.bucket.Get(context.Background(), fullPath)
	if err != nil {
		return nil, internal.ErrRetryable("during bucket get: %s", err)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, internal.ErrRetryable("while reading data from bucket: %s", err)
	}
	return data, nil
}

func (d *DelegatingObjectStore) list(objPath mo.Option[string]) ([]ObjectMeta, error) {
	fullPath := d.rootPath
	if objPath.IsPresent() {
		p, _ := objPath.Get()
		fullPath = path.Join(d.rootPath, p)
	}

	objMetaList := make([]ObjectMeta, 0)
	iterFn := func(attrs objstore.IterObjectAttributes) error {
		lastModified, _ := attrs.LastModified()
		objMetaList = append(objMetaList, ObjectMeta{lastModified, attrs.Name})
		return nil
	}
	err := d.bucket.IterWithAttributes(context.Background(), fullPath, iterFn, objStoreIterOptions(d.bucket)...)
	if err != nil {
		return nil, internal.ErrRetryable("during bucket listing: %s", err)
	}

	return objMetaList, nil
}

// objStoreIterOptions gets IterOptions supported by the storage provider
func objStoreIterOptions(bucket objstore.Bucket) []objstore.IterOption {
	iterOptions := make([]objstore.IterOption, 0)
	requiredOptions := []objstore.IterOption{objstore.WithRecursiveIter(), objstore.WithUpdatedAt()}

	for _, required := range requiredOptions {
		if slices.Contains(bucket.SupportedIterOptions(), required.Type) {
			iterOptions = append(iterOptions, required)
		}
	}
	return iterOptions
}

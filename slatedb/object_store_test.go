package slatedb

import (
	"bytes"
	"context"
	"github.com/naveen246/slatedb-go/slatedb/common"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"io"
	"path"
	"sort"
	"testing"
)

var rootPath = "/root/path"

func TestDelegatingShouldFailPutIfExists(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := newDelegatingObjectStore(rootPath, bucket)

	err := store.putIfNotExists("obj", []byte("data1"))
	assert.NoError(t, err)

	err = store.putIfNotExists("obj", []byte("data2"))
	assert.Error(t, err, common.ErrObjectExists)

	data, err := store.get("obj")
	assert.NoError(t, err)
	assert.True(t, bytes.Equal([]byte("data1"), data))
}

func TestDelegatingShouldGetPut(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := newDelegatingObjectStore(rootPath, bucket)

	err := store.putIfNotExists("obj", []byte("data1"))
	assert.NoError(t, err)

	data, err := store.get("obj")
	assert.NoError(t, err)
	assert.True(t, bytes.Equal([]byte("data1"), data))
}

func TestDelegatingShouldList(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := newDelegatingObjectStore(rootPath, bucket)

	err := store.putIfNotExists("obj", []byte("data1"))
	assert.NoError(t, err)
	err = store.putIfNotExists("foo/bar", []byte("data1"))
	assert.NoError(t, err)

	err = bucket.Upload(context.Background(), "biz/baz", bytes.NewReader([]byte("data1")))
	assert.NoError(t, err)

	list, err := store.list(mo.None[string]())
	assert.NoError(t, err)

	expected := []string{"/root/path/obj", "/root/path/foo/bar"}
	sort.Strings(expected)
	sort.Strings(list)
	assert.Equal(t, expected, list)
}

func TestDelegatingShouldPutWithPrefix(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := newDelegatingObjectStore(rootPath, bucket)

	err := store.putIfNotExists("obj", []byte("data1"))
	assert.NoError(t, err)

	result, err := bucket.Get(context.Background(), path.Join(rootPath, "obj"))
	assert.NoError(t, err)
	data, _ := io.ReadAll(result)
	assert.Equal(t, []byte("data1"), data)
}

package slatedb

import (
	"github.com/naveen246/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"testing"
	"time"
)

func TestPutGetDelete(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	db, err := OpenWithOptions("/tmp/test_kv_store", bucket, testDBOptions(0, 1024))
	assert.NoError(t, err)

	key := []byte("key1")
	value := []byte("value1")
	db.Put(key, value)
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)

	key = []byte("key2")
	value = []byte("value2")
	db.Put(key, value)
	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)

	db.Delete(key)
	val, err = db.Get(key)
	assert.ErrorIs(t, err, common.ErrKeyNotFound)
	db.Close()
}

func testDBOptions(minFilterKeys uint32, l0SSTSizeBytes uint64) DBOptions {
	return DBOptions{
		FlushMS:              100,
		ManifestPollInterval: time.Duration(100),
		MinFilterKeys:        minFilterKeys,
		L0SSTSizeBytes:       l0SSTSizeBytes,
		CompressionCodec:     CompressionNone,
	}
}

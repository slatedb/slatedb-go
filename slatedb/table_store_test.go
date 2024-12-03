package slatedb

import (
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/iter"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"testing"
)

func TestSSTWriter(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	format := defaultSSTableFormat()
	format.blockSize = 32
	format.filterBitsPerKey = 1
	tableStore := newTableStore(bucket, format, "")
	sstID := newSSTableIDCompacted(ulid.Make())

	writer := tableStore.tableWriter(sstID)
	writer.add([]byte("aaaaaaaaaaaaaaaa"), mo.Some([]byte("1111111111111111")))
	writer.add([]byte("bbbbbbbbbbbbbbbb"), mo.Some([]byte("2222222222222222")))
	writer.add([]byte("cccccccccccccccc"), mo.None[[]byte]())
	writer.add([]byte("dddddddddddddddd"), mo.Some([]byte("4444444444444444")))
	sst, err := writer.close()
	assert.NoError(t, err)

	iterator, err := newSSTIterator(sst, tableStore, 1, 1)
	assert.NoError(t, err)
	iter.AssertNextEntry(t, iterator, []byte("aaaaaaaaaaaaaaaa"), []byte("1111111111111111"))
	iter.AssertNextEntry(t, iterator, []byte("bbbbbbbbbbbbbbbb"), []byte("2222222222222222"))
	iter.AssertNextEntry(t, iterator, []byte("cccccccccccccccc"), nil)
	iter.AssertNextEntry(t, iterator, []byte("dddddddddddddddd"), []byte("4444444444444444"))
	_, ok := iterator.NextEntry()
	assert.False(t, ok)
}

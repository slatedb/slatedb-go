package iter

import (
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

type KVIterator interface {
	// Next Returns the next non-deleted key-value pair in the iterator.
	Next() (mo.Option[common.KV], error)

	// NextEntry Returns the next entry in the iterator, which may be a key-value pair or
	// a tombstone of a deleted key-value pair.
	NextEntry() (mo.Option[common.KVDeletable], error)
}

package internal

import "github.com/samber/mo"

type KeyValueIterator interface {
	// Next Returns the next non-deleted key-value pair in the iterator.
	Next() (mo.Option[KeyValue], error)

	// NextEntry Returns the next entry in the iterator, which may be a key-value pair or
	// a tombstone of a deleted key-value pair.
	NextEntry() (mo.Option[KeyValueDeletable], error)
}

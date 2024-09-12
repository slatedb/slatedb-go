package internal

import (
	"github.com/samber/mo"
	"unsafe"
)

// KeyValue Represents a key-value pair known not to be a tombstone.
type KeyValue struct {
	key   []byte
	value []byte
}

// KeyValueDeletable Represents a key-value pair that may be a tombstone.
type KeyValueDeletable struct {
	key      []byte
	valueDel ValueDeletable
}

// ValueDeletable Represents a value that may be a tombstone.
type ValueDeletable struct {
	value       []byte
	isTombstone bool
}

func (v ValueDeletable) size() int64 {
	return int64(unsafe.Sizeof(v))
}

func (v ValueDeletable) intoOption() mo.Option[[]byte] {
	if v.isTombstone {
		return mo.None[[]byte]()
	}
	return mo.Some[[]byte](v.value)
}

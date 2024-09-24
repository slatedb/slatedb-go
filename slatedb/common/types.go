package common

import (
	"github.com/samber/mo"
	"unsafe"
)

// KV Represents a key-value pair known not to be a tombstone.
type KV struct {
	Key   []byte
	Value []byte
}

// KVDeletable Represents a key-value pair that may be a tombstone.
type KVDeletable struct {
	Key      []byte
	ValueDel ValueDeletable
}

// ValueDeletable Represents a value that may be a tombstone.
type ValueDeletable struct {
	Value       []byte
	IsTombstone bool
}

func (v ValueDeletable) Size() int64 {
	return int64(unsafe.Sizeof(v))
}

func (v ValueDeletable) IntoOption() mo.Option[[]byte] {
	if v.IsTombstone {
		return mo.None[[]byte]()
	}
	return mo.Some(v.Value)
}

package common

import (
	"encoding/binary"
	"github.com/samber/mo"
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

// ValueDelFromBytes - if first byte is 1, then return tombstone
// else return with value
func ValueDelFromBytes(b []byte) ValueDeletable {
	if b[0] == 1 {
		return ValueDeletable{IsTombstone: true}
	}

	return ValueDeletable{
		Value:       b[1:],
		IsTombstone: false,
	}
}

// ToBytes - if it is a tombstone return 1 (indicating tombstone) as the only byte
// if it is not a tombstone the value is stored from second byte onwards
func (v ValueDeletable) ToBytes() []byte {
	if v.IsTombstone {
		return []byte{1}
	}
	return append([]byte{0}, v.Value...)
}

func (v ValueDeletable) Size() int64 {
	return int64(binary.Size(v.Value) + binary.Size(v.IsTombstone))
}

func (v ValueDeletable) GetValue() mo.Option[[]byte] {
	if v.IsTombstone {
		return mo.None[[]byte]()
	}
	return mo.Some(v.Value)
}

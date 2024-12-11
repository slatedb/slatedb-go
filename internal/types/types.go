package types

import (
	"encoding/binary"
	"github.com/samber/mo"
)

type Kind int

const (
	KindKeyValue Kind = iota
	KindTombStone
	KindMerge
)

// KeyValue Represents a key-value pair known not to be a tombstone.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// RowEntry Represents a key-value pair that may be a tombstone.
type RowEntry struct {
	Key   []byte
	Value Value

	/*
		// Future Use
		Seq     uint64
		Created time.Time
		Expired time.Time
	*/
}

// Value represents a value in a RowEntry which has a
// Kind which that identifies what kind of Value it represents.
type Value struct {
	Value []byte
	Kind  Kind
}

func (v Value) IsTombstone() bool {
	if v.Kind == KindTombStone {
		return true
	}
	return false
}

// ValueDelFromBytes - if first byte is 1, then return tombstone
// else return with value
func ValueDelFromBytes(b []byte) Value {
	if Kind(b[0]) == KindTombStone {
		return Value{Kind: KindTombStone}
	}

	return Value{
		Value: b[1:],
		Kind:  KindKeyValue,
	}
}

// ToBytes - if it is a tombstone return 1 (indicating tombstone) as the only byte
// if it is not a tombstone the value is stored from second byte onwards
func (v Value) ToBytes() []byte {
	if v.IsTombstone() {
		return []byte{1}
	}
	return append([]byte{0}, v.Value...)
}

func (v Value) Size() int64 {
	return int64(binary.Size(v.Value) + binary.Size(v.IsTombstone))
}

func (v Value) GetValue() mo.Option[[]byte] {
	if v.IsTombstone() {
		return mo.None[[]byte]()
	}
	return mo.Some(v.Value)
}

package types

import (
	"github.com/samber/mo"
)

type Kind byte

const (
	KindKeyValue  Kind = 0x00
	KindTombStone Kind = 0x01
	// TODO(thrawn01): Future MergeOperator
	KindMerge Kind = 0x02
)

// KeyValue represents a key-value pair known not to be a tombstone.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// RowEntry represents a key-value pair that may be a tombstone.
type RowEntry struct {
	Key   []byte
	Value Value

	// // Future Use
	// Seq     uint64
	// Created time.Time
	// Expired time.Time
}

// Value in a RowEntry which has a Kind that identifies
// what kind of Value it represents.
type Value struct {
	Value []byte
	Kind  Kind
}

func (v Value) IsTombstone() bool {
	return v.Kind == KindTombStone
}

// ValueFromBytes - if first byte is 0x01, then return tombstone
// else return with value
func ValueFromBytes(b []byte) Value {
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
		return []byte{byte(KindTombStone)}
	}
	return append([]byte{byte(KindKeyValue)}, v.Value...)
}

func (v Value) GetValue() mo.Option[[]byte] {
	if v.IsTombstone() {
		return mo.None[[]byte]()
	}
	return mo.Some(v.Value)
}

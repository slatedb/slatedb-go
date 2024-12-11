package block

import (
	"encoding/binary"
	"errors"
	"github.com/slatedb/slatedb-go/internal/types"
)

var RowCodecV0 rowCodecV0

type RowFlags uint8

const (
	FlagTombstone RowFlags = 1 << iota
	FlagHasExpire
	FlagHasCreate
)

type Row struct {
	KeyPrefixLen uint16
	KeySuffix    []byte
	Seq          uint64
	// TODO: Convert Expire and Create to time.Time
	ExpireAt  *int64
	CreatedAt *int64
	Value     types.Value
}

// FullKey restores the full key by prepending the prefix to the key suffix.
// Keys in a RowCodecV0 are stored with prefix stripped off to compress the storage size.
func (r Row) FullKey(prefix []byte) []byte {
	result := make([]byte, int(r.KeyPrefixLen)+len(r.KeySuffix))
	copy(result[:r.KeyPrefixLen], prefix[:r.KeyPrefixLen])
	copy(result[r.KeyPrefixLen:], r.KeySuffix)
	return result
}

func (r Row) Flags() RowFlags {
	var flags RowFlags
	if r.Value.IsTombstone() {
		flags |= FlagTombstone
	}
	if r.ExpireAt != nil {
		flags |= FlagHasExpire
	}
	if r.CreatedAt != nil {
		flags |= FlagHasCreate
	}
	return flags
}

func (r Row) Size() int {
	size := 2 + 2 + len(r.KeySuffix) + 8 + 1 // KeyPrefixLen + keySuffixLen + KeySuffix + Seq + Flags
	if r.ExpireAt != nil {
		size += 8
	}
	if r.CreatedAt != nil {
		size += 8
	}
	if !r.Value.IsTombstone() {
		size += 4 + len(r.Value.Value) // value_len + value
	}
	return size
}

type rowCodecV0 struct{}

// Encode key and value using the binary codec for SlateDB row representation
// using the `v0` encoding scheme.
//
// The `v0` codec for the key is (for non-tombstones):
//
// ```txt
//  |---------------------------------------------------------------------------------------------------------------------|
//  |     uint16     |    uint16      |  []byte     | uint64  | uint8     | int64     | int64     | uint32    |  []byte   |
//  |----------------|----------------|-------------|---------|-----------|-----------|-----------|-----------|-----------|
//  | KeyPrefixLen   | KeySuffixLen   | KeySuffix   | seq     | flags     | expireAt  | createdAt | valueLen  | value     |
//  |---------------------------------------------------------------------------------------------------------------------|
// ```
//
// And for tombstones (flags & Tombstone == 1):
//
//  ```txt
//  |----------------------------------------------------------|-----------|-----------|
//  |     uint16     |    uint16      |  []byte     | uint64   | uint8     | int64     |
//  |----------------|----------------|-------------|----------|-----------|-----------|
//  | KeyPrefixLen   | KeySuffixLen   |  KeySuffix  | seq      | flags     | createdAt |
//  |----------------------------------------------------------------------------------|
//  ```
//
// | Field            | Type     | Description                                            |
// |------------------|----------|--------------------------------------------------------|
// | `KeyPrefixLen`   | `uint16` | Length of the key prefix                               |
// | `KeySuffixLen`   | `uint16` | Length of the key suffix                               |
// | `KeySuffix`      | `[]byte` | Suffix of the key                                      |
// | `seq`            | `uint64` | Sequence Number                                        |
// | `flags`          | `uint8`  | Flags of the row                                       |
// | `expireAt`       | `int64`  | Optional, only has value when flags & FlagHasExpire    |
// | `createdAt`      | `int64`  | Optional, only has value when flags & FlagHasCreate    |
// | `value_len`      | `uint32` | Length of the value                                    |
// | `value`          | `[]byte` | Value bytes                                            |
//
// NOTE: both expireAt and createdAt are epoch
func (c rowCodecV0) Encode(r Row) []byte {
	output := make([]byte, r.Size())
	var offset int

	// Encode KeyPrefixLen and KeySuffixLen
	binary.BigEndian.PutUint16(output[offset:], r.KeyPrefixLen)
	offset += 2
	binary.BigEndian.PutUint16(output[offset:], uint16(len(r.KeySuffix)))
	offset += 2

	// Encode KeySuffix
	copy(output[offset:], r.KeySuffix)
	offset += len(r.KeySuffix)

	// Encode seq
	binary.BigEndian.PutUint64(output[offset:], r.Seq)
	offset += 8

	// Encode flags
	output[offset] = uint8(r.Flags())
	offset++

	// Encode expire_ts and create_ts if present
	if r.ExpireAt != nil {
		binary.BigEndian.PutUint64(output[offset:], uint64(*r.ExpireAt))
		offset += 8
	}
	if r.CreatedAt != nil {
		binary.BigEndian.PutUint64(output[offset:], uint64(*r.CreatedAt))
		offset += 8
	}

	// Encode value for non-tombstones
	if !r.Value.IsTombstone() {
		binary.BigEndian.PutUint32(output[offset:], uint32(len(r.Value.Value)))
		offset += 4
		copy(output[offset:], r.Value.Value)
	}

	return output
}

func (c rowCodecV0) Decode(data []byte) (*Row, error) {
	if len(data) < 13 { // Minimum size: KeyPrefixLen + KeySuffixLen + Seq + Flags
		return nil, errors.New("RowCodecV0 corrupt: data length too short to decode a row")
	}

	var r Row
	offset := 0

	// Decode KeyPrefixLen and KeySuffixLen
	r.KeyPrefixLen = binary.BigEndian.Uint16(data[offset:])
	offset += 2
	keySuffixLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	// Decode KeySuffix
	if len(data[offset:]) < int(keySuffixLen) {
		return nil, errors.New("RowCodecV0 corrupt: data length too short for key suffix")
	}
	r.KeySuffix = make([]byte, keySuffixLen)
	copy(r.KeySuffix, data[offset:offset+int(keySuffixLen)])
	offset += int(keySuffixLen)

	// Decode Seq
	r.Seq = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Decode flags
	flags := RowFlags(data[offset])
	offset++

	// Decode expire_ts and create_ts if present
	if flags&FlagHasExpire != 0 {
		if len(data[offset:]) < 8 {
			return nil, errors.New("RowCodecV0 corrupt: data length too short for expire")
		}
		expire := int64(binary.BigEndian.Uint64(data[offset:]))
		r.ExpireAt = &expire
		offset += 8
	}
	if flags&FlagHasCreate != 0 {
		if len(data[offset:]) < 8 {
			return nil, errors.New("RowCodecV0 corrupt: data length too short for create")
		}
		create := int64(binary.BigEndian.Uint64(data[offset:]))
		r.CreatedAt = &create
		offset += 8
	}

	// Decode value for non-tombstones
	if flags&FlagTombstone == 0 {
		if len(data[offset:]) < 4 {
			return nil, errors.New("RowCodecV0 corrupt: data length too short for for value length")
		}
		valueLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		if len(data[offset:]) < int(valueLen) {
			return nil, errors.New("RowCodecV0 corrupt: data length too short for for value")
		}
		value := make([]byte, valueLen)
		copy(value, data[offset:offset+int(valueLen)])
		r.Value = types.Value{Value: value}
	} else {
		r.Value = types.Value{Kind: types.KindTombStone}
	}

	return &r, nil
}

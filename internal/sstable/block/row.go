package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/slatedb/slatedb-go/internal/types"
	"math"
	"time"
)

var v0RowCodec v0Codec

type v0RowFlags uint8

const (
	FlagTombstone v0RowFlags = 1 << iota
	FlagHasExpire
	FlagHasCreate

	v0ErrPrefix = "corrupt v0 row: "
)

type v0Row struct {
	KeyPrefixLen uint16
	KeySuffix    []byte
	Seq          uint64
	ExpireAt     time.Time
	CreatedAt    time.Time
	Value        types.Value
}

// FullKey restores the full key by prepending the prefix to the key suffix.
// Keys in a v0RowCodec are stored with prefix stripped off to reduce the storage size.
func (r v0Row) FullKey(prefix []byte) []byte {
	result := make([]byte, int(r.KeyPrefixLen)+len(r.KeySuffix))
	copy(result[:r.KeyPrefixLen], prefix[:r.KeyPrefixLen])
	copy(result[r.KeyPrefixLen:], r.KeySuffix)
	return result
}

func (r v0Row) ToValue() types.Value {
	if r.Value.IsTombstone() {
		return types.Value{Kind: types.KindTombStone}
	}
	return types.Value{Kind: types.KindKeyValue, Value: r.Value.Value}
}

func (r v0Row) Flags() v0RowFlags {
	var flags v0RowFlags
	if r.Value.IsTombstone() {
		flags |= FlagTombstone
	}
	if r.ExpireAt.Nanosecond() != 0 {
		flags |= FlagHasExpire
	}
	if r.CreatedAt.Nanosecond() != 0 {
		flags |= FlagHasCreate
	}
	return flags
}

func (r v0Row) Size() int {
	size := 2 + 2 + len(r.KeySuffix) + 8 + 1 // KeyPrefixLen + keySuffixLen + KeySuffix + Seq + Flags
	if r.ExpireAt.Nanosecond() != 0 {
		size += 8
	}
	if r.CreatedAt.Nanosecond() != 0 {
		size += 8
	}
	if !r.Value.IsTombstone() {
		size += 4 + len(r.Value.Value) // value_len + value
	}
	return size
}

type v0Codec struct{}

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
func (c v0Codec) Encode(r v0Row) []byte {
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

	// Encode ExpireAt and CreatedAt if present
	if r.ExpireAt.Nanosecond() != 0 {
		binary.BigEndian.PutUint64(output[offset:], uint64(r.ExpireAt.UnixMilli()))
		offset += 8
	}
	if r.CreatedAt.Nanosecond() != 0 {
		binary.BigEndian.PutUint64(output[offset:], uint64(r.CreatedAt.UnixMilli()))
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

func (c v0Codec) Decode(data []byte) (*v0Row, error) {
	if len(data) < 13 { // Minimum size: KeyPrefixLen + KeySuffixLen + Seq + Flags
		return nil, errors.New(v0ErrPrefix + "data length too short to decode a row")
	}

	var offset int
	var r v0Row

	// Decode KeyPrefixLen and KeySuffixLen
	r.KeyPrefixLen = binary.BigEndian.Uint16(data[offset:])
	offset += 2
	keySuffixLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	// Decode KeySuffix
	if len(data[offset:]) < int(keySuffixLen) {
		return nil, errors.New(v0ErrPrefix + "data length too short for key suffix")
	}
	r.KeySuffix = make([]byte, keySuffixLen)
	copy(r.KeySuffix, data[offset:offset+int(keySuffixLen)])
	offset += int(keySuffixLen)

	// Decode Seq
	r.Seq = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Decode flags
	flags := v0RowFlags(data[offset])
	offset++

	// Decode expire_ts and create_ts if present
	if flags&FlagHasExpire != 0 {
		if len(data[offset:]) < 8 {
			return nil, errors.New(v0ErrPrefix + "data length too short for expire")
		}
		expire := int64(binary.BigEndian.Uint64(data[offset:]))
		r.ExpireAt = time.UnixMilli(expire)
		offset += 8
	}
	if flags&FlagHasCreate != 0 {
		if len(data[offset:]) < 8 {
			return nil, errors.New(v0ErrPrefix + "data length too short for create")
		}
		create := int64(binary.BigEndian.Uint64(data[offset:]))
		r.CreatedAt = time.UnixMilli(create)
		offset += 8
	}

	// Decode value for non-tombstones
	if flags&FlagTombstone == 0 {
		if len(data[offset:]) < 4 {
			return nil, errors.New(v0ErrPrefix + "data length too short for for value length")
		}
		valueLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		if len(data[offset:]) < int(valueLen) {
			return nil, errors.New(v0ErrPrefix + "data length too short for for value")
		}
		value := make([]byte, valueLen)
		copy(value, data[offset:offset+int(valueLen)])
		r.Value = types.Value{Value: value}
	} else {
		r.Value = types.Value{Kind: types.KindTombStone}
	}

	return &r, nil
}

// PeekAtKey returns a v0Row with only the KeyPrefixLen and KeySuffix populated where
// the KeySuffix is a sub slice of the provided []byte.
func (c v0Codec) PeekAtKey(data []byte) (v0Row, error) {
	var offset int
	var r v0Row

	// Decode KeyPrefixLen and KeySuffixLen
	r.KeyPrefixLen = binary.BigEndian.Uint16(data[offset:])
	offset += 2
	keySuffixLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	if len(data[offset:]) < int(keySuffixLen) {
		return v0Row{}, errors.New(v0ErrPrefix + "data length too short for key suffix")
	}
	r.KeySuffix = data[offset : offset+int(keySuffixLen)]
	return r, nil
}

// computePrefix calculates the length of the common prefix between two byte slices.
// Source: https://users.rust-lang.org/t/how-to-find-common-prefix-of-two-byte-slices-effectively/25815/4
func computePrefix(lhs, rhs []byte) uint16 {
	return computePrefixChunks(lhs, rhs, 128)
}

// computePrefixChunks calculates the length of the common prefix between two byte slices using chunk-based comparison.
func computePrefixChunks(lhs, rhs []byte, chunkSize int) uint16 {
	minLen := int(math.Min(float64(len(lhs)), float64(len(rhs))))
	chunks := minLen / chunkSize

	// Compare chunks
	var off int
	for i := 0; i < chunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if !bytes.Equal(lhs[start:end], rhs[start:end]) {
			break
		}
		off += chunkSize
	}

	// Compare remaining bytes
	for off < minLen && lhs[off] == rhs[off] {
		off++
	}

	return uint16(off)
}

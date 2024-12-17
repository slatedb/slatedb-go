package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"math"
	"time"
)

var v0RowCodec v0Codec

type v0RowFlags uint8

const (
	flagTombstone v0RowFlags = 1 << iota
	flagHasExpire
	flagHasCreate

	v0ErrPrefix = "corrupt v0 row: "
)

type Row struct {
	Seq       uint64
	ExpireAt  time.Time
	CreatedAt time.Time
	Value     types.Value

	// Used by v0Codec, might consider moving into a separate
	// v0Row structure if future row versions are radically different
	keyPrefixLen uint16
	keySuffix    []byte
}

func (r Row) ToValue() types.Value {
	if r.Value.IsTombstone() {
		return types.Value{Kind: types.KindTombStone}
	}
	return types.Value{Kind: types.KindKeyValue, Value: r.Value.Value}
}

// V0EstimateBlockSize estimates the block size that will result given the
// provided list of types.KeyValue. This estimate assumes no compression is used.
// This function is useful in tests to calculate the block size needed to force
// the creation of multiple blocks.
func V0EstimateBlockSize(kv []types.KeyValue) uint64 {
	b := Builder{}

	// The minimum block size includes all the required offset and length fields
	result := b.curBlockSize()

	for _, kv := range kv {
		r := Row{
			Value:     types.Value{Value: kv.Value},
			keySuffix: kv.Key,
		}
		result += v0Size(r)
		result += common.SizeOfUint16 // The size of a single uint16 offset
	}
	return uint64(result + common.SizeOfUint32) // The size of the checksum
}

// v0FullKey restores the full key by prepending the prefix to the key suffix.
// Keys in a v0RowCodec are stored with prefix stripped off to reduce the storage size.
//
// NOTE: We don't store the full key in the Row to save space, it is up to the
// caller to keep track of the first valid key in the block.
func v0FullKey(r Row, prefix []byte) []byte {
	assert.True(r.keyPrefixLen <= uint16(len(prefix)),
		"row key prefix length %d; exceeds prefix length '%d'", r.keyPrefixLen, len(prefix))
	result := make([]byte, int(r.keyPrefixLen)+len(r.keySuffix))
	copy(result[:r.keyPrefixLen], prefix[:r.keyPrefixLen])
	copy(result[r.keyPrefixLen:], r.keySuffix)
	return result
}

func v0Flags(r Row) v0RowFlags {
	var flags v0RowFlags
	if r.Value.IsTombstone() {
		flags |= flagTombstone
	}
	if r.ExpireAt.Nanosecond() != 0 {
		flags |= flagHasExpire
	}
	if r.CreatedAt.Nanosecond() != 0 {
		flags |= flagHasCreate
	}
	return flags
}

func v0Size(r Row) int {
	size := 2 + 2 + len(r.keySuffix) + 8 + 1 // keyPrefixLen + keySuffixLen + keySuffix + Seq + Flags
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
//
//	|---------------------------------------------------------------------------------------------------------------------|
//	|     uint16     |    uint16      |  []byte     | uint64  | uint8     | int64     | int64     | uint32    |  []byte   |
//	|----------------|----------------|-------------|---------|-----------|-----------|-----------|-----------|-----------|
//	| KeyPrefixLen   | KeySuffixLen   | KeySuffix   | seq     | flags     | expireAt  | createdAt | valueLen  | value     |
//	|---------------------------------------------------------------------------------------------------------------------|
//
// ```
//
// And for tombstones (flags & Tombstone == 1):
//
//	```txt
//	|----------------------------------------------------------|-----------|-----------|
//	|     uint16     |    uint16      |  []byte     | uint64   | uint8     | int64     |
//	|----------------|----------------|-------------|----------|-----------|-----------|
//	| KeyPrefixLen   | KeySuffixLen   |  KeySuffix  | seq      | flags     | createdAt |
//	|----------------------------------------------------------------------------------|
//	```
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
func (c v0Codec) Encode(r Row) []byte {
	output := make([]byte, v0Size(r))
	var offset int

	// Encode keyPrefixLen and KeySuffixLen
	binary.BigEndian.PutUint16(output[offset:], r.keyPrefixLen)
	offset += 2
	binary.BigEndian.PutUint16(output[offset:], uint16(len(r.keySuffix)))
	offset += 2

	// Encode keySuffix
	copy(output[offset:], r.keySuffix)
	offset += len(r.keySuffix)

	// Encode seq
	binary.BigEndian.PutUint64(output[offset:], r.Seq)
	offset += 8

	// Encode flags
	output[offset] = uint8(v0Flags(r))
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

func (c v0Codec) Decode(data []byte, firstKey []byte) (*Row, error) {
	if len(data) < 13 { // Minimum size: keyPrefixLen + KeySuffixLen + Seq + Flags
		return nil, errors.New(v0ErrPrefix + "data length too short to decode a row")
	}

	var offset int
	var r Row

	// Decode keyPrefixLen and KeySuffixLen
	r.keyPrefixLen = binary.BigEndian.Uint16(data[offset:])
	offset += 2
	keySuffixLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	if r.keyPrefixLen > uint16(len(firstKey)) {
		return nil, errors.New(v0ErrPrefix + "key prefix length exceeds length of first key in block")
	}

	// Decode keySuffix
	if len(data[offset:]) < int(keySuffixLen) {
		return nil, errors.New(v0ErrPrefix + "key suffix length exceeds length of block")
	}
	r.keySuffix = make([]byte, keySuffixLen)
	copy(r.keySuffix, data[offset:offset+int(keySuffixLen)])
	offset += int(keySuffixLen)

	// Decode Seq
	r.Seq = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Decode flags
	flags := v0RowFlags(data[offset])
	offset++

	// Decode expire_ts and create_ts if present
	if flags&flagHasExpire != 0 {
		if len(data[offset:]) < 8 {
			return nil, errors.New(v0ErrPrefix + "data length too short for expire")
		}
		expire := int64(binary.BigEndian.Uint64(data[offset:]))
		r.ExpireAt = time.UnixMilli(expire)
		offset += 8
	}
	if flags&flagHasCreate != 0 {
		if len(data[offset:]) < 8 {
			return nil, errors.New(v0ErrPrefix + "data length too short for create")
		}
		create := int64(binary.BigEndian.Uint64(data[offset:]))
		r.CreatedAt = time.UnixMilli(create)
		offset += 8
	}

	// Decode value for non-tombstones
	if flags&flagTombstone == 0 {
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

// PeekAtKey returns a Row with only the keyPrefixLen and keySuffix populated where
// the keySuffix is a sub slice of the provided []byte.
func (c v0Codec) PeekAtKey(data []byte, firstKey []byte) (Row, error) {
	var offset int
	var r Row

	if len(data) < 4 { // Minimum size: keyPrefixLen + KeySuffixLen
		return Row{}, errors.New(v0ErrPrefix + "data length too short to peek at row")
	}

	// Decode keyPrefixLen and KeySuffixLen
	r.keyPrefixLen = binary.BigEndian.Uint16(data[offset:])
	offset += 2
	keySuffixLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	if r.keyPrefixLen > uint16(len(firstKey)) {
		return Row{}, errors.New(v0ErrPrefix + "key prefix length exceeds length of first key in block")
	}

	if len(data[offset:]) < int(keySuffixLen) {
		return Row{}, errors.New(v0ErrPrefix + "key suffix length exceeds length of block")
	}
	r.keySuffix = data[offset : offset+int(keySuffixLen)]
	return r, nil
}

// computePrefixLen calculates the length of the common prefix between two byte slices.
// Source: https://users.rust-lang.org/t/how-to-find-common-prefix-of-two-byte-slices-effectively/25815/4
func computePrefixLen(lhs, rhs []byte) uint16 {
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

package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"hash/crc32"
	"math"
)

var (
	ErrEmptyBlock = errors.New("empty block")
)

const (
	Tombstone = math.MaxUint32
)

type Block struct {
	FirstKey []byte
	Data     []byte
	Offsets  []uint16
}

// Encode encodes the Block into a byte slice using the following format
//
// +-----------------------------------------------+
// |               Block                           |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  Block.Data                             |  |
// |  |  (List of KeyValues)                    |  |
// |  |  +-----------------------------------+  |  |
// |  |  | KeyValue Pair                     |  |  |
// |  |  +-----------------------------------+  |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |  +-----------------------------------------+  |
// |  |  Block.Offsets                          |  |
// |  |  +-----------------------------------+  |  |
// |  |  |  Offset of KeyValue (2 bytes)     |  |  |
// |  |  +-----------------------------------+  |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |  +-----------------------------------------+  |
// |  |  Number of Offsets (2 bytes)            |  |
// |  +-----------------------------------------+  |
// |  |  Checksum (4 bytes)                     |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
func Encode(b *Block, codec compress.Codec) ([]byte, error) {
	bufSize := len(b.Data) + len(b.Offsets)*common.SizeOfUint16 + common.SizeOfUint16

	buf := make([]byte, 0, bufSize)
	buf = append(buf, b.Data...)

	for _, offset := range b.Offsets {
		buf = binary.BigEndian.AppendUint16(buf, offset)
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(b.Offsets)))

	compressed, err := compress.Encode(buf, codec)
	if err != nil {
		return nil, err
	}

	// Make a new buffer exactly the size of the compressed plus the checksum
	buf = make([]byte, 0, len(compressed)+common.SizeOfUint32)
	buf = append(buf, compressed...)
	buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(compressed))
	return buf, nil
}

// Decode converts the encoded byte slice into the provided Block
// TODO(thrawn01): The asserts here should be returned as errors as its impossible
//  to know which block is corrupt without context, nor is it possible to gracefully skip
//  corrupt blocks if these assertions fail.
func Decode(b *Block, input []byte, codec compress.Codec) error {
	assert.True(len(input) > 6, "corrupt block; block is too small; must be at least 6 bytes")

	// last 4 bytes hold the checksum
	checksumIndex := len(input) - common.SizeOfUint32
	compressed := input[:checksumIndex]
	if binary.BigEndian.Uint32(input[checksumIndex:]) != crc32.ChecksumIEEE(compressed) {
		return common.ErrChecksumMismatch
	}

	buf, err := compress.Decode(compressed, codec)
	if err != nil {
		return err
	}

	assert.True(len(buf) > common.SizeOfUint16,
		"corrupt block; uncompressed block is too small; must be at least %d bytes", common.SizeOfUint16)

	// The last 2 bytes hold the offset count
	offsetCountIndex := len(buf) - common.SizeOfUint16
	offsetCount := binary.BigEndian.Uint16(buf[offsetCountIndex:])

	offsetStartIndex := offsetCountIndex - (int(offsetCount) * common.SizeOfUint16)
	assert.True(offsetStartIndex >= 0,
		"corrupt block; offset index %d cannot be negative", offsetStartIndex)
	offsets := make([]uint16, 0, offsetCount)

	for i := 0; i < int(offsetCount); i++ {
		index := offsetStartIndex + (i * common.SizeOfUint16)
		assert.True(index >= 0 && index <= len(buf), "corrupt block; block offset[%d] is invalid", index)
		offsets = append(offsets, binary.BigEndian.Uint16(buf[index:]))
	}

	b.Data = buf[:offsetStartIndex]
	b.Offsets = offsets

	assert.True(len(b.Offsets) != 0, "corrupt block; block Block.Offsets must be greater than 0")

	// Extract the first key in the block
	keyLen := binary.BigEndian.Uint16(b.Data[b.Offsets[0]:])
	b.FirstKey = b.Data[b.Offsets[0]+2 : b.Offsets[0]+2+keyLen]

	return nil
}

type Builder struct {
	offsets   []uint16
	data      []byte
	blockSize uint64
	firstKey  []byte
}

// NewBuilder builds a block of key values in the following
// Block.Data format along with the Block.Offsets which
// point to the beginning of each key/value.
//
// ## Example output from block.PrettyPrint()
//
// Offset: 0
//   uint16(8) - 2 bytes
//   []byte("database") - 8 bytes
//   uint32(9) - 4 bytes
//   []byte("internals") - 9 bytes
// Offset: 23
//   uint16(14) - 2 bytes
//   []byte("data-intensive") - 14 bytes
//   uint32(12) - 4 bytes
//   []byte("applications") - 12 bytes
// Offset: 55
//   uint16(7) - 2 bytes
//   []byte("deleted") - 7 bytes
//   uint32(4294967295) - 4 bytes
//
//
// +-----------------------------------------------+
// |               KeyValue                        |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  Key Length (2 bytes)                   |  |
// |  +-----------------------------------------+  |
// |  |  Key                                    |  |
// |  +-----------------------------------------+  |
// |  |  Value Length (4 bytes)                 |  |
// |  +-----------------------------------------+  |
// |  |  Value                                  |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
//
// If it is a tombstone then KeyValue is represented as.
//
// +-----------------------------------------------+
// |               KeyValue (Tombstone)            |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  Key Length (2 bytes)                   |  |
// |  +-----------------------------------------+  |
// |  |  Key                                    |  |
// |  +-----------------------------------------+  |
// |  |  Tombstone (4 bytes)                    |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
//
// The returned Block struct contains the Data as described
// and the Offsets of each key value in the block.
func NewBuilder(blockSize uint64) *Builder {
	return &Builder{
		offsets:   make([]uint16, 0),
		data:      make([]byte, 0),
		blockSize: blockSize,
	}
}

func (b *Builder) estimatedSize() int {
	return common.SizeOfUint16 + // number of key-value pairs in the block
		(len(b.offsets) * common.SizeOfUint16) + // offsets
		len(b.data) // key-value pairs
}

func (b *Builder) Add(key []byte, value mo.Option[[]byte]) bool {
	assert.True(len(key) > 0, "key must not be empty")

	valueLen := 0
	val, ok := value.Get()
	if ok {
		valueLen = len(val)
	}
	newSize := b.estimatedSize() + len(key) + valueLen + (common.SizeOfUint16 * 2) + common.SizeOfUint32

	// If adding the key-value pair would exceed the block size limit, don't add it.
	// (Unless the block is empty, in which case, allow the block to exceed the limit.)
	if uint64(newSize) > b.blockSize && !b.IsEmpty() {
		return false
	}

	if b.firstKey == nil {
		b.firstKey = bytes.Clone(key)
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))

	// If value is present then append KeyLength(uint16), Key, ValueLength(uint32), value.
	// if value is absent then append KeyLength(uint16), Key, Tombstone(uint32)
	b.data = binary.BigEndian.AppendUint16(b.data, uint16(len(key)))
	b.data = append(b.data, key...)
	if valueLen > 0 {
		b.data = binary.BigEndian.AppendUint32(b.data, uint32(valueLen))
		b.data = append(b.data, val...)
	} else {
		b.data = binary.BigEndian.AppendUint32(b.data, Tombstone)
	}
	return true
}

func (b *Builder) IsEmpty() bool {
	return len(b.offsets) == 0
}

func (b *Builder) Build() (*Block, error) {
	if b.IsEmpty() {
		return nil, ErrEmptyBlock
	}
	return &Block{
		FirstKey: b.firstKey,
		Offsets:  b.offsets,
		Data:     b.data,
	}, nil
}

func PrettyPrint(block *Block) string {
	buf := new(bytes.Buffer)
	it := NewIterator(block)
	for _, offset := range block.Offsets {
		kv, ok := it.NextEntry()
		if !ok {
			_, _ = fmt.Fprintf(buf, "WARN: there are more offsets than blocks")
		}
		_, _ = fmt.Fprintf(buf, "Offset: %d\n", offset)
		_, _ = fmt.Fprintf(buf, "  uint16(%d) - 2 bytes\n", len(kv.Key))
		_, _ = fmt.Fprintf(buf, "  []byte(\"%s\") - %d bytes\n", Truncate(kv.Key, 30), len(kv.Key))
		if kv.Value.IsTombstone {
			_, _ = fmt.Fprintf(buf, "  uint32(%d) - 4 bytes\n", Tombstone)
		} else {
			v := kv.Value.Value
			_, _ = fmt.Fprintf(buf, "  uint32(%d) - 4 bytes\n", len(v))
			_, _ = fmt.Fprintf(buf, "  []byte(\"%s\") - %d bytes\n", Truncate(v, 30), len(v))
		}
	}
	if _, ok := it.NextEntry(); ok {
		_, _ = fmt.Fprintf(buf, "WARN: there are more blocks than offsets")
	}
	return buf.String()
}

// Truncate takes a given byte slice and truncates it to the provided
// length appending "..." to the end if the slice was truncated and returning
// the result as a string.
func Truncate(data []byte, maxLength int) string {
	if len(data) <= maxLength {
		return string(data)
	}
	maxLength -= 3
	truncated := make([]byte, maxLength)
	copy(truncated, data[:maxLength])
	return fmt.Sprintf("%s...", truncated)
}

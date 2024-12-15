package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"hash/crc32"
)

var (
	ErrEmptyBlock = errors.New("empty block")
)

type Block struct {
	FirstKey []byte
	Data     []byte
	Offsets  []uint16
}

// Encode encodes the Block into a byte slice using the following format
//
// NOTE: The first key in the block is a "full key" which means it
// shares no prefix with any previous keys. Subsequent keys in the block store
// only store the suffix of the first if they share a common prefix with the first
// key in the block, If they don't share a common prefix, then the suffix holds
// the full key.
// +-----------------------------------------------+
// |               Block                           |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  Block.Data                             |  |
// |  |  (List of KeyValues)                    |  |
// |  |  +-----------------------------------+  |  |
// |  |  | KeyValue Format (See row.go)      |  |  |
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
func Decode(b *Block, input []byte, codec compress.Codec) error {
	if len(input) < 6 {
		return errors.New("corrupt block: block is too small; must be at least 6 bytes")
	}

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

	if len(buf) < common.SizeOfUint16 {
		return errors.New("corrupt block: uncompressed block is too small; must be at least 2 bytes")
	}

	// The last 2 bytes hold the offset count
	offsetCountIndex := len(buf) - common.SizeOfUint16
	offsetCount := binary.BigEndian.Uint16(buf[offsetCountIndex:])

	offsetStartIndex := offsetCountIndex - (int(offsetCount) * common.SizeOfUint16)
	if offsetStartIndex <= 0 {
		return fmt.Errorf("corrupt block: invalid index offset '%d'; cannot be negative", offsetStartIndex)
	}
	offsets := make([]uint16, 0, offsetCount)

	for i := 0; i < int(offsetCount); i++ {
		index := offsetStartIndex + (i * common.SizeOfUint16)
		if index <= 0 || index >= len(buf) {
			return fmt.Errorf("corrupt block: block offset[%d] is invalid", index)
		}
		offsets = append(offsets, binary.BigEndian.Uint16(buf[index:]))
	}

	b.Data = buf[:offsetStartIndex]
	b.Offsets = offsets

	if len(b.Offsets) == 0 {
		return fmt.Errorf("corrupt block: Block.Offsets must be greater than 0")
	}

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

// NewBuilder builds a block of key values in the v0RowCodec
// format along with the Block.Offsets which point to the
// beginning of each key/value.
//
// See v0RowCodec for on disk format of the key values.
func NewBuilder(blockSize uint64) *Builder {
	return &Builder{
		offsets:   make([]uint16, 0),
		data:      make([]byte, 0),
		blockSize: blockSize,
	}
}

func (b *Builder) curBlockSize() int {
	return common.SizeOfUint16 + // number of key-value pairs in the block
		(len(b.offsets) * common.SizeOfUint16) + // offsets
		len(b.data) // Row entries already in the block
}

func (b *Builder) Add(key []byte, row Row) bool {
	assert.True(len(key) > 0, "key must not be empty")
	row.keyPrefixLen = computePrefixLen(b.firstKey, key)
	row.keySuffix = key[row.keyPrefixLen:]

	// If adding the key-value pair would exceed the block size limit, don't add it.
	// (Unless the block is empty, in which case, allow the block to exceed the limit.)
	// NOTE: This is the current block size, plus the size of a new offset in block.Offsets,
	// plus the size of the new row to be added.
	if uint64(b.curBlockSize()+common.SizeOfUint16+v0Size(row)) > b.blockSize && !b.IsEmpty() {
		return false
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))
	b.data = append(b.data, v0RowCodec.Encode(row)...)

	if b.firstKey == nil {
		b.firstKey = bytes.Clone(key)
	}
	return true
}

func (b *Builder) AddValue(key []byte, value []byte) bool {
	if len(value) == 0 {
		return b.Add(key, Row{Value: types.Value{Kind: types.KindTombStone}})
	}
	return b.Add(key, Row{Value: types.Value{Value: value}})
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
			if warn := it.Warnings(); warn != nil {
				_, _ = fmt.Fprintf(buf, "WARN: %s\n", warn.String())
			} else {
				_, _ = fmt.Fprintf(buf, "WARN: there are more offsets than blocks")
			}
		}
		_, _ = fmt.Fprintf(buf, "Offset: %d\n", offset)
		_, _ = fmt.Fprintf(buf, "    Key: []byte(\"%s\") - %d bytes\n", Truncate(kv.Key, 30), len(kv.Key))
		if kv.Value.IsTombstone() {
			_, _ = fmt.Fprintf(buf, "    IsTombstone\n")
		} else {
			v := kv.Value.Value
			_, _ = fmt.Fprintf(buf, "  Value: []byte(\"%s\") - %d bytes\n", Truncate(v, 30), len(v))
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

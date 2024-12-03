package block

import (
	"encoding/binary"
	"errors"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"math"
)

var (
	ErrEmptyBlock = errors.New("empty block")
)

const (
	Tombstone = math.MaxUint32
)

type Block struct {
	Data    []byte
	Offsets []uint16
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
// |  |  ...                                 |  |  |
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
// +-----------------------------------------------+
func Encode(b *Block) []byte {
	bufSize := len(b.Data) + len(b.Offsets)*common.SizeOfUint16 + common.SizeOfUint16

	buf := make([]byte, 0, bufSize)
	buf = append(buf, b.Data...)

	for _, offset := range b.Offsets {
		buf = binary.BigEndian.AppendUint16(buf, offset)
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(b.Offsets)))
	return buf
}

// Decode converts the encoded byte slice into the provided Block
func Decode(b *Block, bytes []byte) error {
	assert.True(len(bytes) > 6, "invalid block; block is too small; must be at least 6 bytes")

	// the last 2 bytes hold the offset count
	offsetCountIndex := len(bytes) - common.SizeOfUint16
	offsetCount := binary.BigEndian.Uint16(bytes[offsetCountIndex:])

	offsetStartIndex := offsetCountIndex - (int(offsetCount) * common.SizeOfUint16)
	offsets := make([]uint16, 0, offsetCount)

	for i := 0; i < int(offsetCount); i++ {
		index := offsetStartIndex + (i * common.SizeOfUint16)
		offsets = append(offsets, binary.BigEndian.Uint16(bytes[index:]))
	}

	b.Data = bytes[:offsetStartIndex]
	b.Offsets = offsets
	return nil
}

type Builder struct {
	offsets   []uint16
	data      []byte
	blockSize uint64
}

// NewBuilder builds a block of key values in the following format
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
	newSize := b.estimatedSize() + len(key) + valueLen + (common.SizeOfUint16 * 2) + common.SizeOfUint32InBytes

	// If adding the key-value pair would exceed the block size limit, don't add it.
	// (Unless the block is empty, in which case, allow the block to exceed the limit.)
	if uint64(newSize) > b.blockSize && !b.IsEmpty() {
		return false
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
		Data:    b.data,
		Offsets: b.offsets,
	}, nil
}

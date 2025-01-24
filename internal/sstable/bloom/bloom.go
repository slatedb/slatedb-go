package bloom

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"hash/fnv"

	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

type Filter struct {
	NumProbes uint16
	Data      []byte
}

// HasKey returns true if the key might exist in the bloom filter, false if it definitely does not
func (f *Filter) HasKey(key []byte) bool {
	if len(f.Data) == 0 {
		return false
	}

	probes := probesForKey(filterHash(key), f.NumProbes, uint32(len(f.Data)*8))
	for _, p := range probes {
		if !checkBit(uint64(p), f.Data) {
			return false
		}
	}
	return true
}

// Encode encodes the bloom filter into a byte slice using binary.BigEndian
// in the following format
//
// +-----------------------------------------------+
// |               Bloom Filter                    |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  Num of Probes (2 bytes)                |  |
// |  +-----------------------------------------+  |
// |  |  Bit Array (N * bitsPerKey)             |  |
// |  |  +-----------------------------------+  |  |
// |  |  |  Bit 0                            |  |  |
// |  |  |  Bit 1                            |  |  |
// |  |  |  ...                              |  |  |
// |  |  +-----------------------------------+  |  |
// |  +-----------------------------------------+  |
// |  |  Checksum (4 bytes)                     |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
func Encode(f Filter, codec compress.Codec) ([]byte, error) {
	buf := make([]byte, 2+len(f.Data))
	binary.BigEndian.PutUint16(buf[:2], f.NumProbes)
	copy(buf[2:], f.Data)

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

// Decode decodes the bloom filter from the provided byte slice using binary.BigEndian
func Decode(data []byte, codec compress.Codec) (Filter, error) {
	if len(data) < 2 {
		return Filter{}, errors.New("corrupt filter: filter is too small; must be at least 2 bytes")
	}

	checksumIndex := len(data) - common.SizeOfUint32
	compressed := data[:checksumIndex]
	if binary.BigEndian.Uint32(data[checksumIndex:]) != crc32.ChecksumIEEE(compressed) {
		return Filter{}, common.ErrChecksumMismatch
	}

	buf, err := compress.Decode(compressed, codec)
	if err != nil {
		return Filter{}, err
	}

	numProbes := binary.BigEndian.Uint16(buf[:2])
	return Filter{
		NumProbes: numProbes,
		Data:      buf[2:],
	}, nil
}

type Builder struct {
	keyHashes  []uint64
	bitsPerKey uint32
}

func NewBuilder(bitsPerKey uint32) *Builder {
	return &Builder{
		keyHashes:  make([]uint64, 0),
		bitsPerKey: bitsPerKey,
	}
}

// Add adds a new key to the bloom filter. This method
// assumes the keys added are all unique.
func (b *Builder) Add(key []byte) {
	b.keyHashes = append(b.keyHashes, filterHash(key))
}

// Build builds the bloom filter using enhanced double hashing
func (b *Builder) Build() Filter {
	if len(b.keyHashes) == 0 {
		return Filter{}
	}

	numProbes := optimalNumProbes(b.bitsPerKey)
	bytes := filterBytes(uint32(len(b.keyHashes)), b.bitsPerKey)
	bits := uint32(bytes * 8)
	buf := make([]byte, bytes)

	for _, k := range b.keyHashes {
		probes := probesForKey(k, numProbes, bits)
		for _, p := range probes {
			setBit(uint64(p), buf)
		}
	}

	return Filter{
		NumProbes: numProbes,
		Data:      buf,
	}
}

func filterBytes(numKeys uint32, bitsPerKey uint32) uint64 {
	filterBits := numKeys * bitsPerKey
	// compute filter bytes rounded up to the number of bytes required to fit the filter
	return uint64((filterBits + 7) / 8)
}

func filterHash(key []byte) uint64 {
	hash := fnv.New64()
	hash.Write(key)
	return hash.Sum64()
}

func probesForKey(keyHash uint64, numProbes uint16, filtrBits uint32) []uint32 {
	// implements enhanced double hashing from:
	// https://www.khoury.northeastern.edu/~pete/pub/bloom-filters-verification.pdf
	probes := make([]uint32, numProbes)
	filterBits := uint64(filtrBits)
	h := ((keyHash << 32) >> 32) % filterBits // lower 32 bits of hash
	delta := (keyHash >> 32) % filterBits     // higher 32 bits of hash
	for i := 0; i < int(numProbes); i++ {
		delta = (delta + uint64(i)) % filterBits
		probes[i] = uint32(h)
		h = (h + delta) % filterBits
	}
	return probes
}

func checkBit(bit uint64, buf []byte) bool {
	byt := bit / 8
	bitInByte := bit % 8
	return (buf[byt] & (1 << bitInByte)) != 0
}

func setBit(bit uint64, buf []byte) {
	byt := bit / 8
	bitInByte := bit % 8
	buf[byt] |= 1 << bitInByte
}

func optimalNumProbes(bitsPerKey uint32) uint16 {
	// bits_per_key * ln(2)
	// https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
	return uint16(float32(bitsPerKey) * 0.69)
}

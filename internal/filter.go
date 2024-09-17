package internal

import (
	"encoding/binary"
	"hash/fnv"
)

// ------------------------------------------------
// BloomFilter
// ------------------------------------------------

type BloomFilter struct {
	numProbes uint16
	buffer    []byte
}

func decodeBytesToBloomFilter(buf []byte) *BloomFilter {
	numProbes := binary.BigEndian.Uint16(buf)
	return &BloomFilter{
		numProbes: numProbes,
		buffer:    buf[2:],
	}
}

func (b *BloomFilter) encodeToBytes() []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, b.numProbes)
	buf = append(buf, b.buffer...)
	return buf
}

func (b *BloomFilter) filterBits() uint32 {
	return uint32(len(b.buffer) * 8)
}

func (b *BloomFilter) hasKey(key []byte) bool {
	probes := probesForKey(filterHash(key), b.numProbes, b.filterBits())
	for _, p := range probes {
		if !checkBit(uint64(p), b.buffer) {
			return false
		}
	}
	return true
}

// ------------------------------------------------
// BloomFilterBuilder
// ------------------------------------------------

type BloomFilterBuilder struct {
	bitsPerKey uint32
	keyHashes  []uint64
}

func newBloomFilterBuilder(bitsPerKey uint32) *BloomFilterBuilder {
	return &BloomFilterBuilder{
		bitsPerKey: bitsPerKey,
		keyHashes:  make([]uint64, 0),
	}
}

func (b *BloomFilterBuilder) addKey(key []byte) {
	b.keyHashes = append(b.keyHashes, filterHash(key))
}

func (b *BloomFilterBuilder) filterBytes(numKeys uint32, bitsPerKey uint32) uint64 {
	filterBits := numKeys * bitsPerKey
	// compute filter bytes rounded up to the number of bytes required to fit the filter
	return uint64((filterBits + 7) / 8)
}

func (b *BloomFilterBuilder) build() *BloomFilter {
	numProbes := optimalNumProbes(b.bitsPerKey)
	filtrBytes := b.filterBytes(uint32(len(b.keyHashes)), b.bitsPerKey)
	filterBits := uint32(filtrBytes * 8)
	buffer := make([]byte, filtrBytes)

	for _, k := range b.keyHashes {
		probes := probesForKey(k, numProbes, filterBits)
		for _, p := range probes {
			setBit(uint64(p), buffer)
		}
	}

	return &BloomFilter{
		numProbes: numProbes,
		buffer:    buffer,
	}
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

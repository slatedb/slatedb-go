package slatedb

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func assertIterNextEntry(t *testing.T, iter KeyValueIterator, key []byte, value []byte) {
	nextEntry, err := iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsPresent())

	entry, _ := nextEntry.Get()
	assert.Equal(t, key, entry.key)
	if value == nil {
		assert.True(t, entry.valueDel.isTombstone)
		assert.Equal(t, []byte(nil), entry.valueDel.value)
	} else {
		assert.False(t, entry.valueDel.isTombstone)
		assert.Equal(t, value, entry.valueDel.value)
	}
}

func assertIterNext(t *testing.T, iter KeyValueIterator, key []byte, value []byte) {
	next, err := iter.Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())

	kv, _ := next.Get()
	assert.Equal(t, key, kv.key)
	assert.Equal(t, value, kv.value)
}

type OrderedBytesGenerator struct {
	suffix []byte
	data   []byte
	min    byte
	max    byte
}

func newOrderedBytesGeneratorWithSuffix(suffix, data []byte) OrderedBytesGenerator {
	return OrderedBytesGenerator{suffix, data, 0, math.MaxUint8}
}

func newOrderedBytesGeneratorWithByteRange(data []byte, min byte, max byte) OrderedBytesGenerator {
	return OrderedBytesGenerator{[]byte{}, data, 0, math.MaxUint8}
}

func newOrderedBytesGenerator(suffix, data []byte, min, max byte) OrderedBytesGenerator {
	return OrderedBytesGenerator{suffix, data, min, max}
}

func (g OrderedBytesGenerator) clone() OrderedBytesGenerator {
	new := g
	new.suffix = append([]byte{}, g.suffix...)
	new.data = append([]byte{}, g.data...)
	return new
}

func (g OrderedBytesGenerator) next() []byte {
	result := make([]byte, 0, len(g.data)+SizeOfUint32InBytes)
	result = append(result, g.data...)
	result = append(result, g.suffix...)
	g.increment()
	return result
}

func (g OrderedBytesGenerator) increment() {
	pos := len(g.data) - 1
	for pos >= 0 && g.data[pos] == g.max {
		g.data[pos] = g.min
		pos -= 1
	}
	if pos >= 0 {
		g.data[pos] += 1
	}
}

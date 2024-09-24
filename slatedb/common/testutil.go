package common

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func AssertIterNextEntry(t *testing.T, iter KeyValueIterator, key []byte, value []byte) {
	nextEntry, err := iter.NextEntry()
	assert.NoError(t, err)
	assert.True(t, nextEntry.IsPresent())

	entry, _ := nextEntry.Get()
	assert.Equal(t, key, entry.Key)
	if value == nil {
		assert.True(t, entry.ValueDel.IsTombstone)
		assert.Equal(t, []byte(nil), entry.ValueDel.Value)
	} else {
		assert.False(t, entry.ValueDel.IsTombstone)
		assert.Equal(t, value, entry.ValueDel.Value)
	}
}

func AssertIterNext(t *testing.T, iter KeyValueIterator, key []byte, value []byte) {
	next, err := iter.Next()
	assert.NoError(t, err)
	assert.True(t, next.IsPresent())

	kv, _ := next.Get()
	assert.Equal(t, key, kv.Key)
	assert.Equal(t, value, kv.Value)
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

func NewOrderedBytesGeneratorWithByteRange(data []byte, min byte, max byte) OrderedBytesGenerator {
	return OrderedBytesGenerator{[]byte{}, data, min, max}
}

func NewOrderedBytesGenerator(suffix, data []byte, min, max byte) OrderedBytesGenerator {
	return OrderedBytesGenerator{suffix, data, min, max}
}

func (g OrderedBytesGenerator) Clone() OrderedBytesGenerator {
	newGen := g
	newGen.suffix = append([]byte{}, g.suffix...)
	newGen.data = append([]byte{}, g.data...)
	return newGen
}

func (g OrderedBytesGenerator) Next() []byte {
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

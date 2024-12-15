package assert

import (
	"fmt"
	"github.com/slatedb/slatedb-go/internal/iter"
	assert2 "github.com/stretchr/testify/assert"
	"testing"
)

func True(condition bool, errMsg string, arg ...any) {
	if !condition {
		panic(fmt.Sprintf("Assertion Failed: %s\n", fmt.Sprintf(errMsg, arg...)))
	}
}

// NextEntry is a test helper to assert the next call to NextEntry() returns the required value
func NextEntry(t *testing.T, iter iter.KVIterator, key []byte, value []byte) {
	t.Helper()
	entry, ok := iter.NextEntry()
	assert2.True(t, ok)
	assert2.Equal(t, key, entry.Key)
	if value == nil {
		assert2.True(t, entry.Value.IsTombstone())
		assert2.Equal(t, []byte(nil), entry.Value.Value)
	} else {
		assert2.False(t, entry.Value.IsTombstone())
		assert2.Equal(t, value, entry.Value.Value)
	}
}

// Next is a test helper to assert the next call to Next() returns the required value
func Next(t *testing.T, iter iter.KVIterator, key []byte, value []byte) bool {
	t.Helper()
	kv, _ := iter.Next()
	//assert.True(t, ok)
	if !assert2.Equal(t, key, kv.Key) {
		return false
	}
	if !assert2.Equal(t, value, kv.Value) {
		return false
	}
	return true
}

package assert

import (
	"context"
	"fmt"
	"testing"

	assert2 "github.com/stretchr/testify/assert"

	"github.com/slatedb/slatedb-go/internal/iter"
)

func True(condition bool, errMsg string, arg ...any) {
	if !condition {
		panic(fmt.Sprintf("Assertion Failed: %s\n", fmt.Sprintf(errMsg, arg...)))
	}
}

// NextEntry is a test helper to verify if iter.Next() returns the required key and value
func NextEntry(t *testing.T, iter iter.KVIterator, key []byte, value []byte) {
	t.Helper()
	entry, ok := iter.Next(context.Background())
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
	e, _ := iter.Next(context.Background())
	//assert.True(t, ok)
	if !assert2.Equal(t, key, e.Key) {
		return false
	}
	if !assert2.Equal(t, value, e.Value.Value) {
		return false
	}
	return true
}

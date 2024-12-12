package types_test

import (
	"errors"
	"fmt"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestErrWarn(t *testing.T) {
	var warn types.ErrWarn

	assert.NoError(t, warn.If())
	warn.Add("warn %s", "me")
	assert.Error(t, warn.If())
	assert.Equal(t, []string{"warn me"}, warn.Warnings)

	warn.Add("no one but %s", "me")
	assert.Error(t, warn.If())
	assert.Equal(t, "warn me\nno one but me", warn.If().Error())

	var target *types.ErrWarn
	assert.True(t, errors.As(fmt.Errorf("wrap: %w", warn.If()), &target))
	assert.Equal(t, "warn me\nno one but me", target.If().Error())
}

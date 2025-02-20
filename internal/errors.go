package internal

import (
	"errors"
	"fmt"
)

var (
	ErrAlreadyExists = errors.New("object already exists in store")
	ErrFenced        = Err("fenced during write; multiple writers not allowed")
)

func Err(f string, args ...any) error {
	return &ExportedInternalError{msg: fmt.Sprintf(f, args...)}
}

func ErrRetryable(f string, args ...any) error {
	return &ExportedRetryableError{msg: fmt.Sprintf(f, args...)}
}

func ErrInvalidArgument(f string, args ...any) error {
	return &ExportedInternalError{msg: fmt.Sprintf(f, args...)}
}

// NOTE(thrawn01): These are called "Exported" as they are intended as type alias
// which are exported in db.go. Once we move all the packages out of slatedb/ this
// should not be needed. Internal code should use `ErrXXXX` functions to creating
// errors and avoid using `ExportedXXX` errors directly.

type ExportedInternalError struct {
	msg string
}

func (e ExportedInternalError) Error() string {
	return e.msg
}

type ExportedRetryableError struct {
	msg string
}

func (e ExportedRetryableError) Error() string {
	return e.msg
}

type ExportedInvalidArgument struct {
	msg string
}

func (e ExportedInvalidArgument) Error() string {
	return e.msg
}

type ExportedKeyNotFound struct {
	Msg string
}

func (e ExportedKeyNotFound) Error() string {
	return e.Msg
}

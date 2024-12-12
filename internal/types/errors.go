package types

import (
	"fmt"
	"strings"
)

type ErrWarn struct {
	Warnings []string
}

func (e *ErrWarn) Error() string {
	return strings.Join(e.Warnings, "\n")
}

func (e *ErrWarn) Is(target error) bool {
	_, ok := target.(*ErrWarn)
	return ok
}

func (e *ErrWarn) Add(s string, arg ...any) {
	e.Warnings = append(e.Warnings, fmt.Sprintf(s, arg...))
}

func (e *ErrWarn) If() error {
	if len(e.Warnings) > 0 {
		return e
	}
	return nil
}

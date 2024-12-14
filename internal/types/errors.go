package types

import (
	"fmt"
	"slices"
	"strings"
)

type ErrWarn struct {
	Warnings []string
}

func (e *ErrWarn) Error() string {
	return strings.Join(e.Warnings, "\n")
}

func (e *ErrWarn) String() string {
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

func (e *ErrWarn) Empty() bool {
	return len(e.Warnings) == 0
}

// Merge the provided ErrWarn with this ErrWarn, omitting duplicates
func (e *ErrWarn) Merge(rhs *ErrWarn) {
	if rhs == nil {
		return
	}

	for _, w := range rhs.Warnings {
		if !slices.Contains(e.Warnings, w) {
			e.Warnings = append(e.Warnings, w)
		}
	}
}

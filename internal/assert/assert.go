package assert

import "fmt"

func True(condition bool, errMsg string, arg ...any) {
	if !condition {
		panic(fmt.Sprintf("Assertion Failed: %s\n", fmt.Sprintf(errMsg, arg...)))
	}
}

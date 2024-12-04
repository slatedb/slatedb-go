package common

import "github.com/gammazero/deque"

const (
	// uint16 and uint32 sizes are constant as per https://go.dev/ref/spec#Size_and_alignment_guarantees

	SizeOfUint16 = 2
	SizeOfUint32 = 4
)

type Range struct {
	// The lower bound of the range (inclusive).
	Start uint64

	// The upper bound of the range (exclusive).
	End uint64
}

func AssertTrue(condition bool, errMsg string) {
	if !condition {
		panic(errMsg)
	}
}

type Cloneable[T any] interface {
	Clone() T
}

func CopyDeque[T Cloneable[T]](src *deque.Deque[T]) *deque.Deque[T] {
	dst := deque.New[T]()
	for i := 0; i < src.Len(); i++ {
		dst.PushBack(src.At(i).Clone())
	}
	return dst
}

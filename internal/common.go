package internal

const (
	// uint16 and uint32 sizes are constant as per https://go.dev/ref/spec#Size_and_alignment_guarantees

	SizeOfUint16InBytes = 2
	SizeOfUint32InBytes = 4
)

type Range struct {
	// The lower bound of the range (inclusive).
	start uint64

	// The upper bound of the range (exclusive).
	end uint64
}

func assertTrue(condition bool, errMsg string) {
	if !condition {
		panic(errMsg)
	}
}

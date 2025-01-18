package manifest

import (
	"sync/atomic"

	"github.com/slatedb/slatedb-go/slatedb/state"
)

type Manifest struct {
	Core           *state.CoreDBState
	WriterEpoch    atomic.Uint64
	CompactorEpoch atomic.Uint64
}

type Codec interface {
	Encode(manifest *Manifest) []byte
	Decode(data []byte) (*Manifest, error)
}

package slatedb

import (
	"github.com/slatedb/slatedb-go/slatedb/state"
	"sync/atomic"
)

type Manifest struct {
	core           *state.CoreDBState
	writerEpoch    atomic.Uint64
	compactorEpoch atomic.Uint64
}

type ManifestCodec interface {
	encode(manifest *Manifest) []byte
	decode(data []byte) (*Manifest, error)
}

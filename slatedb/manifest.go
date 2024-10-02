package slatedb

import "sync/atomic"

type Manifest struct {
	core           *CoreDBState
	writerEpoch    atomic.Uint64
	compactorEpoch atomic.Uint64
}

type ManifestCodec interface {
	encode(manifest *Manifest) []byte
	decode(data []byte) (*Manifest, error)
}

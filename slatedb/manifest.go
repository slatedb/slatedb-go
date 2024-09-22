package slatedb

type Manifest struct {
	core           CoreDBState
	writerEpoch    uint64
	compactorEpoch uint64
}

type ManifestCodec interface {
	encode(manifest *Manifest) []byte
	decode(data []byte) (*Manifest, error)
}

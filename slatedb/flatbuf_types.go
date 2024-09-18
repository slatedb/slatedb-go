package slatedb

type FlatBufferManifestCodec struct {
}

func (f FlatBufferManifestCodec) encode(manifest *Manifest) []byte {
	//TODO implement me
	panic("implement me")
}

func (f FlatBufferManifestCodec) decode(data []byte) (*Manifest, error) {
	//TODO implement me
	panic("implement me")
}

func (f FlatBufferManifestCodec) manifest() Manifest {
	return Manifest{}
}

func (f FlatBufferManifestCodec) createFromManifest() []byte {
	return nil
}

type DbFlatBufferBuilder struct {
}

package slatedb

type CompressionCodec int

const (
	CompressionNone CompressionCodec = iota
	CompressionSnappy
	CompressionZlib
)

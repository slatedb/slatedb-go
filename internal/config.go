package internal

type CompressionCodec int

const (
	CompressionNone CompressionCodec = iota
	CompressionSnappy
	CompressionZlib
)

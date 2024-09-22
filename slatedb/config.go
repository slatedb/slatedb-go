package slatedb

type CompressionCodec int

const (
	CompressionNone CompressionCodec = iota
	CompressionSnappy
	CompressionZlib
)

type DBOptions struct {
}

func DefaultOptions() DBOptions {
	return DBOptions{}
}

package slatedb

type CompressionCodec int

const (
	CompressionNone CompressionCodec = iota + 1
	CompressionSnappy
	CompressionZlib
)

type DBOptions struct {
}

func DefaultOptions() DBOptions {
	return DBOptions{}
}

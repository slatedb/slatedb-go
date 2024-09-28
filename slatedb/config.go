package slatedb

import "time"

type CompressionCodec int

const (
	CompressionNone CompressionCodec = iota + 1
	CompressionSnappy
	CompressionZlib
)

// DBOptions Configuration options for the database. These options are set on client startup.
type DBOptions struct {
	FlushMS              uint64
	ManifestPollInterval time.Duration
	MinFilterKeys        uint32
	L0SSTSizeBytes       uint64
	CompactorOptions     *CompactorOptions
	CompressionCodec     CompressionCodec
}

func DefaultDBOptions() DBOptions {
	return DBOptions{
		FlushMS:              100,
		ManifestPollInterval: time.Second * 1,
		MinFilterKeys:        1000,
		L0SSTSizeBytes:       128,
		CompactorOptions:     DefaultCompactorOptions(),
		CompressionCodec:     CompressionNone,
	}
}

type ReadLevel int

const (
	Committed ReadLevel = iota + 1
	Uncommitted
)

// ReadOptions Configuration for client read operations. `ReadOptions` is supplied for each
// read call and controls the behavior of the read.
type ReadOptions struct {
	// The read commit level for read operations.
	ReadLevel ReadLevel
}

func DefaultReadOptions() ReadOptions {
	return ReadOptions{
		ReadLevel: Committed,
	}
}

// WriteOptions Configuration for client write operations. `WriteOptions` is supplied for each
// write call and controls the behavior of the write.
type WriteOptions struct {
	// Whether `put` calls should block until the write has been durably committed
	// to the DB.
	AwaitFlush bool
}

func DefaultWriteOptions() WriteOptions {
	return WriteOptions{
		AwaitFlush: true,
	}
}

type CompactorOptions struct {
	PollInterval time.Duration
	MaxSSTSize   uint64
}

func DefaultCompactorOptions() *CompactorOptions {
	return &CompactorOptions{
		PollInterval: time.Second * 5,
		MaxSSTSize:   1024 * 1024 * 1024,
	}
}

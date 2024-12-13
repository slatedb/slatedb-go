package slatedb

import (
	"github.com/slatedb/slatedb-go/internal/compress"
	"log/slog"
	"time"
)

// DBOptions Configuration opts for the database. These opts are set on client startup.
type DBOptions struct {
	// How frequently to flush the write-ahead log to object storage (in
	// milliseconds).
	//
	// When setting this configuration, users must consider:
	//
	// * **Latency**: The higher the flush interval, the longer it will take for
	//   writes to be committed to object storage. Writers blocking on `put` calls
	//   will wait longer for the write. Readers reading committed writes will also
	//   see data later.
	// * **API cost**: The lower the flush interval, the more frequently PUT calls
	//   will be made to object storage. This can increase your object storage costs.
	//
	// We recommend setting this value based on your cost and latency tolerance. A
	// 100ms flush interval should result in $130/month in PUT costs on S3 standard.
	//
	// Keep in mind that the flush interval does not include the network latency. A
	// 100ms flush interval will result in a 100ms + the time it takes to send the
	// bytes to object storage.
	FlushInterval time.Duration

	// How frequently to poll for new manifest files. Refreshing the manifest file
	// allows writers to detect fencing operations and allows readers to detect newly
	// compacted data.
	ManifestPollInterval time.Duration

	// Write SSTables with a bloom filter if the number of keys in the SSTable
	// is greater than or equal to this value. Reads on small SSTables might be
	// faster without a bloom filter.
	MinFilterKeys uint32

	// The minimum size a memtable needs to be before it is frozen and flushed to
	// L0 object storage. Writes will still be flushed to the object storage WAL
	// (based on FlushInterval) regardless of this value. Memtable sizes are checked
	// every `FlushInterval` Duration.
	//
	// When setting this configuration, users must consider:
	//
	// * **Recovery time**: The larger the L0 SSTable size threshold, the less
	//   frequently it will be written. As a result, the more recovery data there
	//   will be in the WAL if a process restarts.
	// * **Number of L0 SSTs/SRs**: The smaller the L0 SSTable size threshold, the
	//   more SSTs and Sorted Runs there will be. L0 SSTables are not range
	//   partitioned; each is its own sorted table. Similarly, each Sorted Run also
	//   stores the entire keyspace. As such, reads that don't hit the WAL or memtable
	//   may need to scan all L0 SSTables and Sorted Runs. The more there are, the
	//   slower the scan will be.
	// * **Memory usage**: The larger the L0 SSTable size threshold, the larger the
	//   unflushed in-memory memtable will grow. This shouldn't be a concern for most
	//   workloads, but it's worth considering for workloads with very high L0
	//   SSTable sizes.
	// * **API cost**: Smaller L0 SSTable sizes will result in more frequent writes
	//   to object storage. This can increase your object storage costs.
	// * **Secondary reader latency**: Secondary (non-writer) clients only see L0+
	//   writes; they don't see WAL writes. Thus, the higher the L0 SSTable size, the
	//   less frequently they will be written, and the longer it will take for
	//   secondary readers to see new data.
	L0SSTSizeBytes uint64

	// Log used to log database warnings
	Log *slog.Logger

	// Configuration opts for the compactor.
	CompactorOptions *CompactorOptions
	CompressionCodec compress.Codec
}

func DefaultDBOptions() DBOptions {
	return DBOptions{
		FlushInterval:        100 * time.Millisecond,
		ManifestPollInterval: 1 * time.Second,
		MinFilterKeys:        1000,
		L0SSTSizeBytes:       64 * 1024 * 1024,
		CompactorOptions:     DefaultCompactorOptions(),
		CompressionCodec:     compress.CodecNone,
		Log:                  slog.Default(),
	}
}

type ReadLevel int

// Whether reads see only writes that have been committed durably to the DB.  A
// write is considered durably committed if all future calls to read are guaranteed
// to serve the data written by the write, until some later durably committed write
// updates the same key.
const (
	// Committed - Client reads will only see data that's been committed durably to the DB.
	Committed ReadLevel = iota + 1

	// Uncommitted - Clients will see all writes, including those not yet durably committed to the
	// DB.
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
	// The interval at which the compactor checks for a new manifest and decides
	// if a compaction must be scheduled
	PollInterval time.Duration

	// A compacted SSTable's maximum size (in bytes). If more data needs to be
	// written to a Sorted Run during a compaction, a new SSTable will be created
	// in the Sorted Run when this size is exceeded.
	MaxSSTSize uint64
}

func DefaultCompactorOptions() *CompactorOptions {
	return &CompactorOptions{
		PollInterval: 5 * time.Second,
		MaxSSTSize:   1024 * 1024 * 1024,
	}
}

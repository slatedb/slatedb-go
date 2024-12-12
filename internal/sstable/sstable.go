package sstable

import (
	"bytes"
	"github.com/slatedb/slatedb-go/internal/compress"
)

// Info contains meta information on the SSTable when it is serialized.
// This is used when we read SSTable as a slice of bytes from object storage and we want to parse the slice of bytes
// Each SSTable is a list of blocks and each block is a list of KeyValue pairs.
type Info struct {
	// contains the FirstKey of the SSTable
	FirstKey []byte

	// the offset at which SSTableIndex starts when SSTable is serialized.
	// SSTableIndex holds the meta info about each block. SSTableIndex is defined in schemas/sst.fbs
	IndexOffset uint64

	// the length of the SSTableIndex.
	IndexLen uint64

	// the offset at which Bloom filter starts when SSTable is serialized.
	FilterOffset uint64

	// the length of the Bloom filter
	FilterLen uint64

	// the codec used to compress/decompress SSTable before writing/reading from object storage
	CompressionCodec compress.Codec
}

func (info *Info) Clone() *Info {
	return &Info{
		FirstKey:         bytes.Clone(info.FirstKey),
		IndexOffset:      info.IndexOffset,
		IndexLen:         info.IndexLen,
		FilterOffset:     info.FilterOffset,
		FilterLen:        info.FilterLen,
		CompressionCodec: info.CompressionCodec,
	}
}

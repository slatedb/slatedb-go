package sstable

import (
	"encoding/binary"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/compress"
	"hash/crc32"
)

// Info contains meta information on the SSTable when it is serialized.
// This is used when we read SSTable as a slice of bytes from object storage and we want to parse the slice of bytes
// Each SSTable is a list of blocks and each block is a list of KeyValue pairs.
type Info struct {
	// contains the FirstKey of the SSTable
	FirstKey mo.Option[[]byte]

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

// TODO: Make this a package level function
func (info *Info) Encode(buf *[]byte, sstCodec SsTableInfoCodec) {
	data := sstCodec.Encode(info)
	*buf = append(*buf, data...)
	*buf = binary.BigEndian.AppendUint32(*buf, crc32.ChecksumIEEE(data))
}

func (info *Info) Clone() *Info {
	firstKey := mo.None[[]byte]()
	if info.FirstKey.IsPresent() {
		key, _ := info.FirstKey.Get()
		k := make([]byte, len(key))
		copy(k, key)
		firstKey = mo.Some(k)
	}
	return &Info{
		FirstKey:         firstKey,
		IndexOffset:      info.IndexOffset,
		IndexLen:         info.IndexLen,
		FilterOffset:     info.FilterOffset,
		FilterLen:        info.FilterLen,
		CompressionCodec: info.CompressionCodec,
	}
}

// SsTableInfoCodec - implementation of this interface defines how we
// encode sstable.Info to byte slice and decode byte slice back to sstable.Info
// Currently we use FlatBuffers for encoding and decoding.
type SsTableInfoCodec interface {
	Encode(info *Info) []byte
	Decode(data []byte) *Info
}

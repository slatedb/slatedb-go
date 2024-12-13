package sstable

import (
	"bytes"
	"encoding/binary"
	"github.com/google/flatbuffers/go"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/flatbuf"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"hash/crc32"
)

type Index struct {
	Data []byte

	blockMetaT   []*flatbuf.BlockMetaT
	sstableIndex *flatbuf.SsTableIndex
}

func (info *Index) BlockMeta() []*flatbuf.BlockMetaT {
	// Return the cached result if available
	if info.blockMetaT != nil {
		return info.blockMetaT
	}
	info.sstableIndex = flatbuf.GetRootAsSsTableIndex(info.Data, 0)
	info.blockMetaT = info.sstableIndex.UnPack().BlockMeta
	return info.blockMetaT
}

func (info *Index) BlockMetaLength() int {
	if info.sstableIndex != nil {
		return info.sstableIndex.BlockMetaLength()
	}
	info.sstableIndex = flatbuf.GetRootAsSsTableIndex(info.Data, 0)
	return info.sstableIndex.BlockMetaLength()
}

func (info *Index) Clone() *Index {
	data := make([]byte, len(info.Data))
	copy(data, info.Data)
	return &Index{
		Data: data,
	}
}

func SstInfoToFlatBuf(info *Info) *flatbuf.SsTableInfoT {

	return &flatbuf.SsTableInfoT{
		FirstKey:          bytes.Clone(info.FirstKey),
		IndexOffset:       info.IndexOffset,
		IndexLen:          info.IndexLen,
		FilterOffset:      info.FilterOffset,
		FilterLen:         info.FilterLen,
		CompressionFormat: compress.CodecToFlatBuf(info.CompressionCodec),
	}
}

// TODO(thrawn01): Use these instead of the FlatBufferBuilders above

// EncodeInfo encodes the provided Info into flatbuf.SsTableInfoT flat []byte
// format along with a checksum of flatbuf.SsTableInfoT
func EncodeInfo(info *Info) []byte {
	// Encode the Info struct as flatbuf.SsTableInfoT
	builder := flatbuffers.NewBuilder(0)
	firstKey := builder.CreateByteVector(info.FirstKey)

	flatbuf.SsTableInfoStart(builder)
	flatbuf.SsTableInfoAddFirstKey(builder, firstKey)
	flatbuf.SsTableInfoAddIndexOffset(builder, info.IndexOffset)
	flatbuf.SsTableInfoAddIndexLen(builder, info.IndexLen)
	flatbuf.SsTableInfoAddFilterOffset(builder, info.FilterOffset)
	flatbuf.SsTableInfoAddFilterLen(builder, info.FilterLen)
	flatbuf.SsTableInfoAddCompressionFormat(builder, flatbuf.CompressionCodec(info.CompressionCodec))
	infoOffset := flatbuf.SsTableInfoEnd(builder)

	builder.Finish(infoOffset)
	b := builder.FinishedBytes()

	// Add a checksum to the end of the slice
	return binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
}

func DecodeInfo(b []byte) (*Info, error) {
	if len(b) <= common.SizeOfUint32 {
		return nil, common.ErrEmptyBlockMeta
	}

	// last 4 bytes hold the checksum
	checksumIndex := len(b) - common.SizeOfUint32
	checksum := binary.BigEndian.Uint32(b[checksumIndex:])
	if checksum != crc32.ChecksumIEEE(b[:checksumIndex]) {
		return nil, common.ErrChecksumMismatch
	}

	fbInfo := flatbuf.GetRootAsSsTableInfo(b, 0)
	info := &Info{
		FirstKey:         bytes.Clone(fbInfo.FirstKeyBytes()),
		IndexOffset:      fbInfo.IndexOffset(),
		IndexLen:         fbInfo.IndexLen(),
		FilterOffset:     fbInfo.FilterOffset(),
		FilterLen:        fbInfo.FilterLen(),
		CompressionCodec: compress.Codec(fbInfo.CompressionFormat()),
	}
	return info, nil
}

func encodeIndex(index flatbuf.SsTableIndexT) []byte {
	builder := flatbuffers.NewBuilder(0)
	offset := index.Pack(builder)
	builder.Finish(offset)
	return builder.FinishedBytes()
}

// EncodeTable encodes the provided sstable.Table into the
// SSTable format as []byte.
func EncodeTable(table *Table) []byte {
	var result []byte
	for i := 0; i < table.Blocks.Len(); i++ {
		result = append(result, table.Blocks.At(i)...)
	}
	return result
}

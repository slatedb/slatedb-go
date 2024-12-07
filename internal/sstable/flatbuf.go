package sstable

import (
	"bytes"
	"encoding/binary"
	"github.com/google/flatbuffers/go"
	"github.com/slatedb/slatedb-go/gen"
	"github.com/slatedb/slatedb-go/internal/compress"
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
		info.sstableIndex.BlockMetaLength()
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

// FlatBufferSSTableInfoCodec implements SsTableInfoCodec and defines how we
// encode sstable.Info to byte slice and decode byte slice back to sstable.Info
// TODO: Remove once we separate Config and Decoder from SSTableFormat
type FlatBufferSSTableInfoCodec struct{}

func (f FlatBufferSSTableInfoCodec) Encode(info *Info) []byte {
	fbSSTInfo := SstInfoToFlatBuf(info)
	builder := flatbuffers.NewBuilder(0)
	offset := fbSSTInfo.Pack(builder)
	builder.Finish(offset)
	return builder.FinishedBytes()
}

func (f FlatBufferSSTableInfoCodec) Decode(data []byte) *Info {
	info := flatbuf.GetRootAsSsTableInfo(data, 0)
	return SstInfoFromFlatBuf(info)
}

// TODO: Remove
func SstInfoFromFlatBuf(info *flatbuf.SsTableInfo) *Info {
	return &Info{
		FirstKey:         bytes.Clone(info.FirstKeyBytes()),
		IndexOffset:      info.IndexOffset(),
		IndexLen:         info.IndexLen(),
		FilterOffset:     info.FilterOffset(),
		FilterLen:        info.FilterLen(),
		CompressionCodec: compress.CodecFromFlatBuf(info.CompressionFormat()),
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
	flatbuf.SsTableInfoAddCompressionFormat(builder, flatbuf.CompressionFormat(info.CompressionCodec))
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

// TODO(thrawn01): This does not appear to be used anywhere, perhaps we need to add a test
//  for it??????
func decodeIndex(b []byte) *flatbuf.SsTableIndexT {
	return flatbuf.GetRootAsSsTableIndex(b, 0).UnPack()
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

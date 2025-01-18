package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/flatbuf"
	"github.com/slatedb/slatedb-go/slatedb/common"
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

func DecodeIndex(buf []byte, codec compress.Codec) (*Index, error) {
	if len(buf) <= common.SizeOfUint32 {
		return nil, common.ErrEmptyBlockMeta
	}

	checksumIndex := len(buf) - common.SizeOfUint32
	compressed := buf[:checksumIndex]
	if binary.BigEndian.Uint32(buf[checksumIndex:]) != crc32.ChecksumIEEE(compressed) {
		return nil, common.ErrChecksumMismatch
	}

	buf, err := compress.Decode(compressed, codec)
	if err != nil {
		return nil, err
	}

	return &Index{Data: buf}, nil
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

func encodeIndex(index flatbuf.SsTableIndexT, codec compress.Codec) ([]byte, error) {
	builder := flatbuffers.NewBuilder(0)
	offset := index.Pack(builder)
	builder.Finish(offset)

	compressed, err := compress.Encode(builder.FinishedBytes(), codec)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, len(compressed)+common.SizeOfUint32)
	buf = append(buf, compressed...)
	return binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(compressed)), nil
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

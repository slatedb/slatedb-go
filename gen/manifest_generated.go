// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"strconv"
)

type CompressionFormat int8

const (
	CompressionFormatNone   CompressionFormat = 0
	CompressionFormatSnappy CompressionFormat = 1
	CompressionFormatZlib   CompressionFormat = 2
	CompressionFormatLz4    CompressionFormat = 3
	CompressionFormatZstd   CompressionFormat = 4
)

var EnumNamesCompressionFormat = map[CompressionFormat]string{
	CompressionFormatNone:   "None",
	CompressionFormatSnappy: "Snappy",
	CompressionFormatZlib:   "Zlib",
	CompressionFormatLz4:    "Lz4",
	CompressionFormatZstd:   "Zstd",
}

var EnumValuesCompressionFormat = map[string]CompressionFormat{
	"None":   CompressionFormatNone,
	"Snappy": CompressionFormatSnappy,
	"Zlib":   CompressionFormatZlib,
	"Lz4":    CompressionFormatLz4,
	"Zstd":   CompressionFormatZstd,
}

func (v CompressionFormat) String() string {
	if s, ok := EnumNamesCompressionFormat[v]; ok {
		return s
	}
	return "CompressionFormat(" + strconv.FormatInt(int64(v), 10) + ")"
}

type SstRowAttribute int8

const (
	SstRowAttributeFlags SstRowAttribute = 0
)

var EnumNamesSstRowAttribute = map[SstRowAttribute]string{
	SstRowAttributeFlags: "Flags",
}

var EnumValuesSstRowAttribute = map[string]SstRowAttribute{
	"Flags": SstRowAttributeFlags,
}

func (v SstRowAttribute) String() string {
	if s, ok := EnumNamesSstRowAttribute[v]; ok {
		return s
	}
	return "SstRowAttribute(" + strconv.FormatInt(int64(v), 10) + ")"
}

type CompactedSstIdT struct {
	High uint64 `json:"high"`
	Low  uint64 `json:"low"`
}

func (t *CompactedSstIdT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	CompactedSstIdStart(builder)
	CompactedSstIdAddHigh(builder, t.High)
	CompactedSstIdAddLow(builder, t.Low)
	return CompactedSstIdEnd(builder)
}

func (rcv *CompactedSstId) UnPackTo(t *CompactedSstIdT) {
	t.High = rcv.High()
	t.Low = rcv.Low()
}

func (rcv *CompactedSstId) UnPack() *CompactedSstIdT {
	if rcv == nil {
		return nil
	}
	t := &CompactedSstIdT{}
	rcv.UnPackTo(t)
	return t
}

type CompactedSstId struct {
	_tab flatbuffers.Table
}

func GetRootAsCompactedSstId(buf []byte, offset flatbuffers.UOffsetT) *CompactedSstId {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &CompactedSstId{}
	x.Init(buf, n+offset)
	return x
}

func FinishCompactedSstIdBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsCompactedSstId(buf []byte, offset flatbuffers.UOffsetT) *CompactedSstId {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &CompactedSstId{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedCompactedSstIdBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *CompactedSstId) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *CompactedSstId) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *CompactedSstId) High() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *CompactedSstId) MutateHigh(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *CompactedSstId) Low() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *CompactedSstId) MutateLow(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func CompactedSstIdStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func CompactedSstIdAddHigh(builder *flatbuffers.Builder, high uint64) {
	builder.PrependUint64Slot(0, high, 0)
}
func CompactedSstIdAddLow(builder *flatbuffers.Builder, low uint64) {
	builder.PrependUint64Slot(1, low, 0)
}
func CompactedSstIdEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type CompactedSsTableT struct {
	Id   *CompactedSstIdT `json:"id"`
	Info *SsTableInfoT    `json:"info"`
}

func (t *CompactedSsTableT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	idOffset := t.Id.Pack(builder)
	infoOffset := t.Info.Pack(builder)
	CompactedSsTableStart(builder)
	CompactedSsTableAddId(builder, idOffset)
	CompactedSsTableAddInfo(builder, infoOffset)
	return CompactedSsTableEnd(builder)
}

func (rcv *CompactedSsTable) UnPackTo(t *CompactedSsTableT) {
	t.Id = rcv.Id(nil).UnPack()
	t.Info = rcv.Info(nil).UnPack()
}

func (rcv *CompactedSsTable) UnPack() *CompactedSsTableT {
	if rcv == nil {
		return nil
	}
	t := &CompactedSsTableT{}
	rcv.UnPackTo(t)
	return t
}

type CompactedSsTable struct {
	_tab flatbuffers.Table
}

func GetRootAsCompactedSsTable(buf []byte, offset flatbuffers.UOffsetT) *CompactedSsTable {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &CompactedSsTable{}
	x.Init(buf, n+offset)
	return x
}

func FinishCompactedSsTableBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsCompactedSsTable(buf []byte, offset flatbuffers.UOffsetT) *CompactedSsTable {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &CompactedSsTable{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedCompactedSsTableBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *CompactedSsTable) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *CompactedSsTable) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *CompactedSsTable) Id(obj *CompactedSstId) *CompactedSstId {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(CompactedSstId)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *CompactedSsTable) Info(obj *SsTableInfo) *SsTableInfo {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(SsTableInfo)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func CompactedSsTableStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func CompactedSsTableAddId(builder *flatbuffers.Builder, id flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(id), 0)
}
func CompactedSsTableAddInfo(builder *flatbuffers.Builder, info flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(info), 0)
}
func CompactedSsTableEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type SsTableInfoT struct {
	FirstKey          []byte            `json:"first_key"`
	IndexOffset       uint64            `json:"index_offset"`
	IndexLen          uint64            `json:"index_len"`
	FilterOffset      uint64            `json:"filter_offset"`
	FilterLen         uint64            `json:"filter_len"`
	CompressionFormat CompressionFormat `json:"compression_format"`
	RowAttributes     []SstRowAttribute `json:"row_attributes"`
}

func (t *SsTableInfoT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	firstKeyOffset := flatbuffers.UOffsetT(0)
	if t.FirstKey != nil {
		firstKeyOffset = builder.CreateByteString(t.FirstKey)
	}
	rowAttributesOffset := flatbuffers.UOffsetT(0)
	if t.RowAttributes != nil {
		rowAttributesLength := len(t.RowAttributes)
		SsTableInfoStartRowAttributesVector(builder, rowAttributesLength)
		for j := rowAttributesLength - 1; j >= 0; j-- {
			builder.PrependInt8(int8(t.RowAttributes[j]))
		}
		rowAttributesOffset = builder.EndVector(rowAttributesLength)
	}
	SsTableInfoStart(builder)
	SsTableInfoAddFirstKey(builder, firstKeyOffset)
	SsTableInfoAddIndexOffset(builder, t.IndexOffset)
	SsTableInfoAddIndexLen(builder, t.IndexLen)
	SsTableInfoAddFilterOffset(builder, t.FilterOffset)
	SsTableInfoAddFilterLen(builder, t.FilterLen)
	SsTableInfoAddCompressionFormat(builder, t.CompressionFormat)
	SsTableInfoAddRowAttributes(builder, rowAttributesOffset)
	return SsTableInfoEnd(builder)
}

func (rcv *SsTableInfo) UnPackTo(t *SsTableInfoT) {
	t.FirstKey = rcv.FirstKeyBytes()
	t.IndexOffset = rcv.IndexOffset()
	t.IndexLen = rcv.IndexLen()
	t.FilterOffset = rcv.FilterOffset()
	t.FilterLen = rcv.FilterLen()
	t.CompressionFormat = rcv.CompressionFormat()
	rowAttributesLength := rcv.RowAttributesLength()
	t.RowAttributes = make([]SstRowAttribute, rowAttributesLength)
	for j := 0; j < rowAttributesLength; j++ {
		t.RowAttributes[j] = rcv.RowAttributes(j)
	}
}

func (rcv *SsTableInfo) UnPack() *SsTableInfoT {
	if rcv == nil {
		return nil
	}
	t := &SsTableInfoT{}
	rcv.UnPackTo(t)
	return t
}

type SsTableInfo struct {
	_tab flatbuffers.Table
}

func GetRootAsSsTableInfo(buf []byte, offset flatbuffers.UOffsetT) *SsTableInfo {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &SsTableInfo{}
	x.Init(buf, n+offset)
	return x
}

func FinishSsTableInfoBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsSsTableInfo(buf []byte, offset flatbuffers.UOffsetT) *SsTableInfo {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &SsTableInfo{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedSsTableInfoBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *SsTableInfo) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *SsTableInfo) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *SsTableInfo) FirstKey(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *SsTableInfo) FirstKeyLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *SsTableInfo) FirstKeyBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *SsTableInfo) MutateFirstKey(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func (rcv *SsTableInfo) IndexOffset() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SsTableInfo) MutateIndexOffset(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *SsTableInfo) IndexLen() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SsTableInfo) MutateIndexLen(n uint64) bool {
	return rcv._tab.MutateUint64Slot(8, n)
}

func (rcv *SsTableInfo) FilterOffset() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SsTableInfo) MutateFilterOffset(n uint64) bool {
	return rcv._tab.MutateUint64Slot(10, n)
}

func (rcv *SsTableInfo) FilterLen() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SsTableInfo) MutateFilterLen(n uint64) bool {
	return rcv._tab.MutateUint64Slot(12, n)
}

func (rcv *SsTableInfo) CompressionFormat() CompressionFormat {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return CompressionFormat(rcv._tab.GetInt8(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *SsTableInfo) MutateCompressionFormat(n CompressionFormat) bool {
	return rcv._tab.MutateInt8Slot(14, int8(n))
}

func (rcv *SsTableInfo) RowAttributes(j int) SstRowAttribute {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return SstRowAttribute(rcv._tab.GetInt8(a + flatbuffers.UOffsetT(j*1)))
	}
	return 0
}

func (rcv *SsTableInfo) RowAttributesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *SsTableInfo) MutateRowAttributes(j int, n SstRowAttribute) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateInt8(a+flatbuffers.UOffsetT(j*1), int8(n))
	}
	return false
}

func SsTableInfoStart(builder *flatbuffers.Builder) {
	builder.StartObject(7)
}
func SsTableInfoAddFirstKey(builder *flatbuffers.Builder, firstKey flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(firstKey), 0)
}
func SsTableInfoStartFirstKeyVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func SsTableInfoAddIndexOffset(builder *flatbuffers.Builder, indexOffset uint64) {
	builder.PrependUint64Slot(1, indexOffset, 0)
}
func SsTableInfoAddIndexLen(builder *flatbuffers.Builder, indexLen uint64) {
	builder.PrependUint64Slot(2, indexLen, 0)
}
func SsTableInfoAddFilterOffset(builder *flatbuffers.Builder, filterOffset uint64) {
	builder.PrependUint64Slot(3, filterOffset, 0)
}
func SsTableInfoAddFilterLen(builder *flatbuffers.Builder, filterLen uint64) {
	builder.PrependUint64Slot(4, filterLen, 0)
}
func SsTableInfoAddCompressionFormat(builder *flatbuffers.Builder, compressionFormat CompressionFormat) {
	builder.PrependInt8Slot(5, int8(compressionFormat), 0)
}
func SsTableInfoAddRowAttributes(builder *flatbuffers.Builder, rowAttributes flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(6, flatbuffers.UOffsetT(rowAttributes), 0)
}
func SsTableInfoStartRowAttributesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func SsTableInfoEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type BlockMetaT struct {
	Offset   uint64 `json:"offset"`
	FirstKey []byte `json:"first_key"`
}

func (t *BlockMetaT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	firstKeyOffset := flatbuffers.UOffsetT(0)
	if t.FirstKey != nil {
		firstKeyOffset = builder.CreateByteString(t.FirstKey)
	}
	BlockMetaStart(builder)
	BlockMetaAddOffset(builder, t.Offset)
	BlockMetaAddFirstKey(builder, firstKeyOffset)
	return BlockMetaEnd(builder)
}

func (rcv *BlockMeta) UnPackTo(t *BlockMetaT) {
	t.Offset = rcv.Offset()
	t.FirstKey = rcv.FirstKeyBytes()
}

func (rcv *BlockMeta) UnPack() *BlockMetaT {
	if rcv == nil {
		return nil
	}
	t := &BlockMetaT{}
	rcv.UnPackTo(t)
	return t
}

type BlockMeta struct {
	_tab flatbuffers.Table
}

func GetRootAsBlockMeta(buf []byte, offset flatbuffers.UOffsetT) *BlockMeta {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &BlockMeta{}
	x.Init(buf, n+offset)
	return x
}

func FinishBlockMetaBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsBlockMeta(buf []byte, offset flatbuffers.UOffsetT) *BlockMeta {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &BlockMeta{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedBlockMetaBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *BlockMeta) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *BlockMeta) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *BlockMeta) Offset() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *BlockMeta) MutateOffset(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *BlockMeta) FirstKey(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *BlockMeta) FirstKeyLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *BlockMeta) FirstKeyBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *BlockMeta) MutateFirstKey(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func BlockMetaStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func BlockMetaAddOffset(builder *flatbuffers.Builder, offset uint64) {
	builder.PrependUint64Slot(0, offset, 0)
}
func BlockMetaAddFirstKey(builder *flatbuffers.Builder, firstKey flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(firstKey), 0)
}
func BlockMetaStartFirstKeyVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func BlockMetaEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type SsTableIndexT struct {
	BlockMeta []*BlockMetaT `json:"block_meta"`
}

func (t *SsTableIndexT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	blockMetaOffset := flatbuffers.UOffsetT(0)
	if t.BlockMeta != nil {
		blockMetaLength := len(t.BlockMeta)
		blockMetaOffsets := make([]flatbuffers.UOffsetT, blockMetaLength)
		for j := 0; j < blockMetaLength; j++ {
			blockMetaOffsets[j] = t.BlockMeta[j].Pack(builder)
		}
		SsTableIndexStartBlockMetaVector(builder, blockMetaLength)
		for j := blockMetaLength - 1; j >= 0; j-- {
			builder.PrependUOffsetT(blockMetaOffsets[j])
		}
		blockMetaOffset = builder.EndVector(blockMetaLength)
	}
	SsTableIndexStart(builder)
	SsTableIndexAddBlockMeta(builder, blockMetaOffset)
	return SsTableIndexEnd(builder)
}

func (rcv *SsTableIndex) UnPackTo(t *SsTableIndexT) {
	blockMetaLength := rcv.BlockMetaLength()
	t.BlockMeta = make([]*BlockMetaT, blockMetaLength)
	for j := 0; j < blockMetaLength; j++ {
		x := BlockMeta{}
		rcv.BlockMeta(&x, j)
		t.BlockMeta[j] = x.UnPack()
	}
}

func (rcv *SsTableIndex) UnPack() *SsTableIndexT {
	if rcv == nil {
		return nil
	}
	t := &SsTableIndexT{}
	rcv.UnPackTo(t)
	return t
}

type SsTableIndex struct {
	_tab flatbuffers.Table
}

func GetRootAsSsTableIndex(buf []byte, offset flatbuffers.UOffsetT) *SsTableIndex {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &SsTableIndex{}
	x.Init(buf, n+offset)
	return x
}

func FinishSsTableIndexBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsSsTableIndex(buf []byte, offset flatbuffers.UOffsetT) *SsTableIndex {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &SsTableIndex{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedSsTableIndexBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *SsTableIndex) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *SsTableIndex) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *SsTableIndex) BlockMeta(obj *BlockMeta, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *SsTableIndex) BlockMetaLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func SsTableIndexStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func SsTableIndexAddBlockMeta(builder *flatbuffers.Builder, blockMeta flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(blockMeta), 0)
}
func SsTableIndexStartBlockMetaVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func SsTableIndexEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type ManifestV1T struct {
	ManifestId         uint64               `json:"manifest_id"`
	WriterEpoch        uint64               `json:"writer_epoch"`
	CompactorEpoch     uint64               `json:"compactor_epoch"`
	WalIdLastCompacted uint64               `json:"wal_id_last_compacted"`
	WalIdLastSeen      uint64               `json:"wal_id_last_seen"`
	L0LastCompacted    *CompactedSstIdT     `json:"l0_last_compacted"`
	L0                 []*CompactedSsTableT `json:"l0"`
	Compacted          []*SortedRunT        `json:"compacted"`
	Snapshots          []*SnapshotT         `json:"snapshots"`
}

func (t *ManifestV1T) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	l0LastCompactedOffset := t.L0LastCompacted.Pack(builder)
	l0Offset := flatbuffers.UOffsetT(0)
	if t.L0 != nil {
		l0Length := len(t.L0)
		l0Offsets := make([]flatbuffers.UOffsetT, l0Length)
		for j := 0; j < l0Length; j++ {
			l0Offsets[j] = t.L0[j].Pack(builder)
		}
		ManifestV1StartL0Vector(builder, l0Length)
		for j := l0Length - 1; j >= 0; j-- {
			builder.PrependUOffsetT(l0Offsets[j])
		}
		l0Offset = builder.EndVector(l0Length)
	}
	compactedOffset := flatbuffers.UOffsetT(0)
	if t.Compacted != nil {
		compactedLength := len(t.Compacted)
		compactedOffsets := make([]flatbuffers.UOffsetT, compactedLength)
		for j := 0; j < compactedLength; j++ {
			compactedOffsets[j] = t.Compacted[j].Pack(builder)
		}
		ManifestV1StartCompactedVector(builder, compactedLength)
		for j := compactedLength - 1; j >= 0; j-- {
			builder.PrependUOffsetT(compactedOffsets[j])
		}
		compactedOffset = builder.EndVector(compactedLength)
	}
	snapshotsOffset := flatbuffers.UOffsetT(0)
	if t.Snapshots != nil {
		snapshotsLength := len(t.Snapshots)
		snapshotsOffsets := make([]flatbuffers.UOffsetT, snapshotsLength)
		for j := 0; j < snapshotsLength; j++ {
			snapshotsOffsets[j] = t.Snapshots[j].Pack(builder)
		}
		ManifestV1StartSnapshotsVector(builder, snapshotsLength)
		for j := snapshotsLength - 1; j >= 0; j-- {
			builder.PrependUOffsetT(snapshotsOffsets[j])
		}
		snapshotsOffset = builder.EndVector(snapshotsLength)
	}
	ManifestV1Start(builder)
	ManifestV1AddManifestId(builder, t.ManifestId)
	ManifestV1AddWriterEpoch(builder, t.WriterEpoch)
	ManifestV1AddCompactorEpoch(builder, t.CompactorEpoch)
	ManifestV1AddWalIdLastCompacted(builder, t.WalIdLastCompacted)
	ManifestV1AddWalIdLastSeen(builder, t.WalIdLastSeen)
	ManifestV1AddL0LastCompacted(builder, l0LastCompactedOffset)
	ManifestV1AddL0(builder, l0Offset)
	ManifestV1AddCompacted(builder, compactedOffset)
	ManifestV1AddSnapshots(builder, snapshotsOffset)
	return ManifestV1End(builder)
}

func (rcv *ManifestV1) UnPackTo(t *ManifestV1T) {
	t.ManifestId = rcv.ManifestId()
	t.WriterEpoch = rcv.WriterEpoch()
	t.CompactorEpoch = rcv.CompactorEpoch()
	t.WalIdLastCompacted = rcv.WalIdLastCompacted()
	t.WalIdLastSeen = rcv.WalIdLastSeen()
	t.L0LastCompacted = rcv.L0LastCompacted(nil).UnPack()
	l0Length := rcv.L0Length()
	t.L0 = make([]*CompactedSsTableT, l0Length)
	for j := 0; j < l0Length; j++ {
		x := CompactedSsTable{}
		rcv.L0(&x, j)
		t.L0[j] = x.UnPack()
	}
	compactedLength := rcv.CompactedLength()
	t.Compacted = make([]*SortedRunT, compactedLength)
	for j := 0; j < compactedLength; j++ {
		x := SortedRun{}
		rcv.Compacted(&x, j)
		t.Compacted[j] = x.UnPack()
	}
	snapshotsLength := rcv.SnapshotsLength()
	t.Snapshots = make([]*SnapshotT, snapshotsLength)
	for j := 0; j < snapshotsLength; j++ {
		x := Snapshot{}
		rcv.Snapshots(&x, j)
		t.Snapshots[j] = x.UnPack()
	}
}

func (rcv *ManifestV1) UnPack() *ManifestV1T {
	if rcv == nil {
		return nil
	}
	t := &ManifestV1T{}
	rcv.UnPackTo(t)
	return t
}

type ManifestV1 struct {
	_tab flatbuffers.Table
}

func GetRootAsManifestV1(buf []byte, offset flatbuffers.UOffsetT) *ManifestV1 {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &ManifestV1{}
	x.Init(buf, n+offset)
	return x
}

func FinishManifestV1Buffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsManifestV1(buf []byte, offset flatbuffers.UOffsetT) *ManifestV1 {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &ManifestV1{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedManifestV1Buffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *ManifestV1) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *ManifestV1) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *ManifestV1) ManifestId() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *ManifestV1) MutateManifestId(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *ManifestV1) WriterEpoch() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *ManifestV1) MutateWriterEpoch(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *ManifestV1) CompactorEpoch() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *ManifestV1) MutateCompactorEpoch(n uint64) bool {
	return rcv._tab.MutateUint64Slot(8, n)
}

func (rcv *ManifestV1) WalIdLastCompacted() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *ManifestV1) MutateWalIdLastCompacted(n uint64) bool {
	return rcv._tab.MutateUint64Slot(10, n)
}

func (rcv *ManifestV1) WalIdLastSeen() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *ManifestV1) MutateWalIdLastSeen(n uint64) bool {
	return rcv._tab.MutateUint64Slot(12, n)
}

func (rcv *ManifestV1) L0LastCompacted(obj *CompactedSstId) *CompactedSstId {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(CompactedSstId)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *ManifestV1) L0(obj *CompactedSsTable, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *ManifestV1) L0Length() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *ManifestV1) Compacted(obj *SortedRun, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(18))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *ManifestV1) CompactedLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(18))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *ManifestV1) Snapshots(obj *Snapshot, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *ManifestV1) SnapshotsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func ManifestV1Start(builder *flatbuffers.Builder) {
	builder.StartObject(9)
}
func ManifestV1AddManifestId(builder *flatbuffers.Builder, manifestId uint64) {
	builder.PrependUint64Slot(0, manifestId, 0)
}
func ManifestV1AddWriterEpoch(builder *flatbuffers.Builder, writerEpoch uint64) {
	builder.PrependUint64Slot(1, writerEpoch, 0)
}
func ManifestV1AddCompactorEpoch(builder *flatbuffers.Builder, compactorEpoch uint64) {
	builder.PrependUint64Slot(2, compactorEpoch, 0)
}
func ManifestV1AddWalIdLastCompacted(builder *flatbuffers.Builder, walIdLastCompacted uint64) {
	builder.PrependUint64Slot(3, walIdLastCompacted, 0)
}
func ManifestV1AddWalIdLastSeen(builder *flatbuffers.Builder, walIdLastSeen uint64) {
	builder.PrependUint64Slot(4, walIdLastSeen, 0)
}
func ManifestV1AddL0LastCompacted(builder *flatbuffers.Builder, l0LastCompacted flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(5, flatbuffers.UOffsetT(l0LastCompacted), 0)
}
func ManifestV1AddL0(builder *flatbuffers.Builder, l0 flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(6, flatbuffers.UOffsetT(l0), 0)
}
func ManifestV1StartL0Vector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func ManifestV1AddCompacted(builder *flatbuffers.Builder, compacted flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(7, flatbuffers.UOffsetT(compacted), 0)
}
func ManifestV1StartCompactedVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func ManifestV1AddSnapshots(builder *flatbuffers.Builder, snapshots flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(8, flatbuffers.UOffsetT(snapshots), 0)
}
func ManifestV1StartSnapshotsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func ManifestV1End(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type SortedRunT struct {
	Id   uint32               `json:"id"`
	Ssts []*CompactedSsTableT `json:"ssts"`
}

func (t *SortedRunT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	sstsOffset := flatbuffers.UOffsetT(0)
	if t.Ssts != nil {
		sstsLength := len(t.Ssts)
		sstsOffsets := make([]flatbuffers.UOffsetT, sstsLength)
		for j := 0; j < sstsLength; j++ {
			sstsOffsets[j] = t.Ssts[j].Pack(builder)
		}
		SortedRunStartSstsVector(builder, sstsLength)
		for j := sstsLength - 1; j >= 0; j-- {
			builder.PrependUOffsetT(sstsOffsets[j])
		}
		sstsOffset = builder.EndVector(sstsLength)
	}
	SortedRunStart(builder)
	SortedRunAddId(builder, t.Id)
	SortedRunAddSsts(builder, sstsOffset)
	return SortedRunEnd(builder)
}

func (rcv *SortedRun) UnPackTo(t *SortedRunT) {
	t.Id = rcv.Id()
	sstsLength := rcv.SstsLength()
	t.Ssts = make([]*CompactedSsTableT, sstsLength)
	for j := 0; j < sstsLength; j++ {
		x := CompactedSsTable{}
		rcv.Ssts(&x, j)
		t.Ssts[j] = x.UnPack()
	}
}

func (rcv *SortedRun) UnPack() *SortedRunT {
	if rcv == nil {
		return nil
	}
	t := &SortedRunT{}
	rcv.UnPackTo(t)
	return t
}

type SortedRun struct {
	_tab flatbuffers.Table
}

func GetRootAsSortedRun(buf []byte, offset flatbuffers.UOffsetT) *SortedRun {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &SortedRun{}
	x.Init(buf, n+offset)
	return x
}

func FinishSortedRunBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsSortedRun(buf []byte, offset flatbuffers.UOffsetT) *SortedRun {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &SortedRun{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedSortedRunBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *SortedRun) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *SortedRun) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *SortedRun) Id() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SortedRun) MutateId(n uint32) bool {
	return rcv._tab.MutateUint32Slot(4, n)
}

func (rcv *SortedRun) Ssts(obj *CompactedSsTable, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *SortedRun) SstsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func SortedRunStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func SortedRunAddId(builder *flatbuffers.Builder, id uint32) {
	builder.PrependUint32Slot(0, id, 0)
}
func SortedRunAddSsts(builder *flatbuffers.Builder, ssts flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(ssts), 0)
}
func SortedRunStartSstsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func SortedRunEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type SnapshotT struct {
	Id                  uint64 `json:"id"`
	ManifestId          uint64 `json:"manifest_id"`
	SnapshotExpireTimeS uint32 `json:"snapshot_expire_time_s"`
}

func (t *SnapshotT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	SnapshotStart(builder)
	SnapshotAddId(builder, t.Id)
	SnapshotAddManifestId(builder, t.ManifestId)
	SnapshotAddSnapshotExpireTimeS(builder, t.SnapshotExpireTimeS)
	return SnapshotEnd(builder)
}

func (rcv *Snapshot) UnPackTo(t *SnapshotT) {
	t.Id = rcv.Id()
	t.ManifestId = rcv.ManifestId()
	t.SnapshotExpireTimeS = rcv.SnapshotExpireTimeS()
}

func (rcv *Snapshot) UnPack() *SnapshotT {
	if rcv == nil {
		return nil
	}
	t := &SnapshotT{}
	rcv.UnPackTo(t)
	return t
}

type Snapshot struct {
	_tab flatbuffers.Table
}

func GetRootAsSnapshot(buf []byte, offset flatbuffers.UOffsetT) *Snapshot {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Snapshot{}
	x.Init(buf, n+offset)
	return x
}

func FinishSnapshotBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsSnapshot(buf []byte, offset flatbuffers.UOffsetT) *Snapshot {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Snapshot{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedSnapshotBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Snapshot) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Snapshot) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Snapshot) Id() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Snapshot) MutateId(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *Snapshot) ManifestId() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Snapshot) MutateManifestId(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *Snapshot) SnapshotExpireTimeS() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Snapshot) MutateSnapshotExpireTimeS(n uint32) bool {
	return rcv._tab.MutateUint32Slot(8, n)
}

func SnapshotStart(builder *flatbuffers.Builder) {
	builder.StartObject(3)
}
func SnapshotAddId(builder *flatbuffers.Builder, id uint64) {
	builder.PrependUint64Slot(0, id, 0)
}
func SnapshotAddManifestId(builder *flatbuffers.Builder, manifestId uint64) {
	builder.PrependUint64Slot(1, manifestId, 0)
}
func SnapshotAddSnapshotExpireTimeS(builder *flatbuffers.Builder, snapshotExpireTimeS uint32) {
	builder.PrependUint32Slot(2, snapshotExpireTimeS, 0)
}
func SnapshotEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

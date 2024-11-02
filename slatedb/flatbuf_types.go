package slatedb

import (
	"encoding/binary"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	flatbuf "github.com/slatedb/slatedb-go/gen"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

// ------------------------------------------------
// SSTableIndexData
// ------------------------------------------------

type SSTableIndexData struct {
	data []byte
}

func newSSTableIndexData(data []byte) *SSTableIndexData {
	return &SSTableIndexData{data: data}
}

func (info *SSTableIndexData) ssTableIndex() *flatbuf.SsTableIndex {
	return flatbuf.GetRootAsSsTableIndex(info.data, 0)
}

func (info *SSTableIndexData) size() int {
	return len(info.data)
}

func (info *SSTableIndexData) clone() *SSTableIndexData {
	data := make([]byte, len(info.data))
	copy(data, info.data)
	return &SSTableIndexData{
		data: data,
	}
}

// ------------------------------------------------
// FlatBufferSSTableInfoCodec
// ------------------------------------------------

// FlatBufferSSTableInfoCodec implements SsTableInfoCodec and defines how we
// encode SSTableInfo to byte slice and decode byte slice back to SSTableInfo
type FlatBufferSSTableInfoCodec struct{}

func (f *FlatBufferSSTableInfoCodec) encode(info *SSTableInfo) []byte {
	builder := flatbuffers.NewBuilder(0)
	dbFBBuilder := newDBFlatBufferBuilder(builder)
	return dbFBBuilder.createSSTInfo(info)
}

func (f *FlatBufferSSTableInfoCodec) decode(data []byte) *SSTableInfo {
	info := flatbuf.GetRootAsSsTableInfo(data, 0)
	return sstInfoFromFlatBuf(info)
}

// ------------------------------------------------
// FlatBufferManifestCodec
// ------------------------------------------------

// FlatBufferManifestCodec implements ManifestCodec and defines how we
// encode Manifest to byte slice and decode byte slice back to Manifest
type FlatBufferManifestCodec struct{}

func (f FlatBufferManifestCodec) encode(manifest *Manifest) []byte {
	builder := flatbuffers.NewBuilder(0)
	dbFlatBufBuilder := newDBFlatBufferBuilder(builder)
	return dbFlatBufBuilder.createManifest(manifest)
}

func (f FlatBufferManifestCodec) decode(data []byte) (*Manifest, error) {
	manifestV1 := flatbuf.GetRootAsManifestV1(data, 0)
	return f.manifest(manifestV1.UnPack()), nil
}

func (f FlatBufferManifestCodec) manifest(manifest *flatbuf.ManifestV1T) *Manifest {
	core := &CoreDBState{
		l0:                    f.parseFlatBufSSTList(manifest.L0),
		compacted:             f.parseFlatBufSortedRuns(manifest.Compacted),
		nextWalSstID:          manifest.WalIdLastSeen + 1,
		lastCompactedWalSSTID: manifest.WalIdLastCompacted,
	}
	l0LastCompacted := f.parseFlatBufSSTId(manifest.L0LastCompacted)
	if l0LastCompacted == ulid.Zero {
		core.l0LastCompacted = mo.None[ulid.ULID]()
	} else {
		core.l0LastCompacted = mo.Some(l0LastCompacted)
	}

	m := &Manifest{}
	m.core = core
	m.writerEpoch.Store(manifest.WriterEpoch)
	m.compactorEpoch.Store(manifest.CompactorEpoch)
	return m
}

func (f FlatBufferManifestCodec) parseFlatBufSSTId(sstID *flatbuf.CompactedSstIdT) ulid.ULID {
	if sstID == nil || (sstID.High == 0 && sstID.Low == 0) {
		return ulid.Zero
	}

	id := make([]byte, 0, 16)
	id = binary.BigEndian.AppendUint64(id, sstID.High)
	id = binary.BigEndian.AppendUint64(id, sstID.Low)

	var ulidID ulid.ULID
	copy(ulidID[:], id)
	return ulidID
}

func (f FlatBufferManifestCodec) parseFlatBufSSTList(fbSSTList []*flatbuf.CompactedSsTableT) []SSTableHandle {
	sstList := make([]SSTableHandle, 0)
	for _, sst := range fbSSTList {
		id := f.parseFlatBufSSTId(sst.Id)
		sstList = append(sstList, SSTableHandle{
			id:   newSSTableIDCompacted(id),
			info: f.parseFlatBufSSTInfo(sst.Info),
		})
	}
	return sstList
}

func (f FlatBufferManifestCodec) parseFlatBufSSTInfo(info *flatbuf.SsTableInfoT) *SSTableInfo {
	firstKey := mo.None[[]byte]()
	keyBytes := info.FirstKey
	if keyBytes != nil {
		firstKey = mo.Some(keyBytes)
	}
	return &SSTableInfo{
		firstKey:         firstKey,
		indexOffset:      info.IndexOffset,
		indexLen:         info.IndexLen,
		filterOffset:     info.FilterOffset,
		filterLen:        info.FilterLen,
		compressionCodec: compressionCodecFromFlatBuf(info.CompressionFormat),
	}
}

func (f FlatBufferManifestCodec) parseFlatBufSortedRuns(fbSortedRuns []*flatbuf.SortedRunT) []SortedRun {
	sortedRuns := make([]SortedRun, 0)
	for _, run := range fbSortedRuns {
		sortedRuns = append(sortedRuns, SortedRun{
			id:      run.Id,
			sstList: f.parseFlatBufSSTList(run.Ssts),
		})
	}
	return sortedRuns
}

// ------------------------------------------------
// DBFlatBufferBuilder
// ------------------------------------------------

type DBFlatBufferBuilder struct {
	builder *flatbuffers.Builder
}

func newDBFlatBufferBuilder(builder *flatbuffers.Builder) DBFlatBufferBuilder {
	return DBFlatBufferBuilder{builder}
}

func (fb *DBFlatBufferBuilder) createManifest(manifest *Manifest) []byte {
	core := manifest.core
	l0 := fb.sstListToFlatBuf(core.l0)
	var l0LastCompacted *flatbuf.CompactedSstIdT
	if core.l0LastCompacted.IsPresent() {
		id, _ := core.l0LastCompacted.Get()
		l0LastCompacted = fb.compactedSSTID(id)
	}
	compacted := fb.sortedRunsToFlatBuf(core.compacted)

	manifestV1 := flatbuf.ManifestV1T{
		ManifestId:         0,
		WriterEpoch:        manifest.writerEpoch.Load(),
		CompactorEpoch:     manifest.compactorEpoch.Load(),
		WalIdLastCompacted: core.lastCompactedWalSSTID,
		WalIdLastSeen:      core.nextWalSstID - 1,
		L0LastCompacted:    l0LastCompacted,
		L0:                 l0,
		Compacted:          compacted,
		Snapshots:          nil,
	}
	manifestOffset := manifestV1.Pack(fb.builder)
	fb.builder.Finish(manifestOffset)
	return fb.builder.FinishedBytes()
}

func (fb *DBFlatBufferBuilder) createSSTInfo(info *SSTableInfo) []byte {
	fbSSTInfo := sstInfoToFlatBuf(info)
	offset := fbSSTInfo.Pack(fb.builder)
	fb.builder.Finish(offset)
	return fb.builder.FinishedBytes()
}

func (fb *DBFlatBufferBuilder) sstListToFlatBuf(sstList []SSTableHandle) []*flatbuf.CompactedSsTableT {
	compactedSSTs := make([]*flatbuf.CompactedSsTableT, 0)
	for _, sst := range sstList {
		compactedSSTs = append(compactedSSTs, fb.compactedSST(sst.id, sst.info))
	}
	return compactedSSTs
}

func (fb *DBFlatBufferBuilder) compactedSST(sstID SSTableID, sstInfo *SSTableInfo) *flatbuf.CompactedSsTableT {
	common.AssertTrue(sstID.typ == Compacted, "cannot pass WAL SST handle to create compacted sst")
	id, err := ulid.Parse(sstID.value)
	if err != nil {
		return nil
	}

	return &flatbuf.CompactedSsTableT{
		Id:   fb.compactedSSTID(id),
		Info: sstInfoToFlatBuf(sstInfo),
	}
}

func (fb *DBFlatBufferBuilder) compactedSSTID(id ulid.ULID) *flatbuf.CompactedSstIdT {
	hi := id.Bytes()[:8]
	lo := id.Bytes()[8:]
	return &flatbuf.CompactedSstIdT{
		High: binary.BigEndian.Uint64(hi),
		Low:  binary.BigEndian.Uint64(lo),
	}
}

func (fb *DBFlatBufferBuilder) sortedRunsToFlatBuf(sortedRuns []SortedRun) []*flatbuf.SortedRunT {
	sortedRunFBs := make([]*flatbuf.SortedRunT, 0)
	for _, sortedRun := range sortedRuns {
		sortedRunFBs = append(sortedRunFBs, &flatbuf.SortedRunT{
			Id:   sortedRun.id,
			Ssts: fb.sstListToFlatBuf(sortedRun.sstList),
		})
	}
	return sortedRunFBs
}

func sstInfoFromFlatBuf(info *flatbuf.SsTableInfo) *SSTableInfo {
	firstKey := mo.None[[]byte]()
	keyBytes := info.FirstKeyBytes()
	if keyBytes != nil {
		firstKey = mo.Some(keyBytes)
	}

	return &SSTableInfo{
		firstKey:         firstKey,
		indexOffset:      info.IndexOffset(),
		indexLen:         info.IndexLen(),
		filterOffset:     info.FilterOffset(),
		filterLen:        info.FilterLen(),
		compressionCodec: compressionCodecFromFlatBuf(info.CompressionFormat()),
	}
}

func sstInfoToFlatBuf(info *SSTableInfo) *flatbuf.SsTableInfoT {
	var firstKey []byte
	if info.firstKey.IsPresent() {
		firstKey, _ = info.firstKey.Get()
	}

	return &flatbuf.SsTableInfoT{
		FirstKey:          firstKey,
		IndexOffset:       info.indexOffset,
		IndexLen:          info.indexLen,
		FilterOffset:      info.filterOffset,
		FilterLen:         info.filterLen,
		CompressionFormat: compressionCodecToFlatBuf(info.compressionCodec),
	}
}

func compressionCodecFromFlatBuf(compressionFormat flatbuf.CompressionFormat) CompressionCodec {
	switch compressionFormat {
	case flatbuf.CompressionFormatNone:
		return CompressionNone
	case flatbuf.CompressionFormatSnappy:
		return CompressionSnappy
	case flatbuf.CompressionFormatZlib:
		return CompressionZlib
	case flatbuf.CompressionFormatLz4:
		return CompressionLz4
	case flatbuf.CompressionFormatZstd:
		return CompressionZstd
	default:
		panic("invalid CompressionFormat")
	}
}

func compressionCodecToFlatBuf(codec CompressionCodec) flatbuf.CompressionFormat {
	switch codec {
	case CompressionNone:
		return flatbuf.CompressionFormatNone
	case CompressionSnappy:
		return flatbuf.CompressionFormatSnappy
	case CompressionZlib:
		return flatbuf.CompressionFormatZlib
	case CompressionLz4:
		return flatbuf.CompressionFormatLz4
	case CompressionZstd:
		return flatbuf.CompressionFormatZstd
	default:
		panic("invalid CompressionCodec")
	}
}

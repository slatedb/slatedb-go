package slatedb

import (
	"encoding/binary"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	flatbuf "github.com/slatedb/slatedb-go/gen"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

type FlatBufferManifestCodec struct {
}

func (f FlatBufferManifestCodec) encode(manifest *Manifest) []byte {
	return f.createFromManifest(manifest)
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

	m := &Manifest{
		core: core,
	}
	m.writerEpoch.Store(manifest.WriterEpoch)
	m.compactorEpoch.Store(manifest.CompactorEpoch)
	return m
}

func (f FlatBufferManifestCodec) createFromManifest(manifest *Manifest) []byte {
	builder := flatbuffers.NewBuilder(0)
	dbFlatBufBuilder := newDBFlatBuilder(builder)
	return dbFlatBufBuilder.createManifest(manifest)
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

func (f FlatBufferManifestCodec) parseFlatBufSSTInfo(info *flatbuf.SsTableInfoT) *SSTableInfoOwned {
	builder := flatbuffers.NewBuilder(0)
	infoOffset := info.Pack(builder)
	builder.Finish(infoOffset)
	return newSSTableInfoOwned(builder.FinishedBytes())
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

type DBFlatBufferBuilder struct {
	builder *flatbuffers.Builder
}

func newDBFlatBuilder(builder *flatbuffers.Builder) DBFlatBufferBuilder {
	return DBFlatBufferBuilder{builder}
}

func (fb DBFlatBufferBuilder) createManifest(manifest *Manifest) []byte {
	core := manifest.core
	l0 := fb.addCompactedSSTs(core.l0)
	var l0LastCompacted *flatbuf.CompactedSstIdT
	if core.l0LastCompacted.IsPresent() {
		id, _ := core.l0LastCompacted.Get()
		l0LastCompacted = fb.addCompactedSSTID(id)
	}
	compacted := fb.addSortedRuns(core.compacted)

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

func (fb DBFlatBufferBuilder) addCompactedSSTs(sstList []SSTableHandle) []*flatbuf.CompactedSsTableT {
	compactedSSTs := make([]*flatbuf.CompactedSsTableT, 0)
	for _, sst := range sstList {
		compactedSSTs = append(compactedSSTs, fb.addCompactedSST(sst.id, sst.info))
	}
	return compactedSSTs
}

func (fb DBFlatBufferBuilder) addCompactedSST(sstID SSTableID, sstInfo *SSTableInfoOwned) *flatbuf.CompactedSsTableT {
	common.AssertTrue(sstID.typ == Compacted, "cannot pass WAL SST handle to create compacted sst")
	id, err := ulid.Parse(sstID.value)
	if err != nil {
		return nil
	}

	return &flatbuf.CompactedSsTableT{
		Id:   fb.addCompactedSSTID(id),
		Info: sstInfo.borrow().UnPack(),
	}
}

func (fb DBFlatBufferBuilder) addCompactedSSTID(id ulid.ULID) *flatbuf.CompactedSstIdT {
	hi := id.Bytes()[:8]
	lo := id.Bytes()[8:]
	return &flatbuf.CompactedSstIdT{
		High: binary.BigEndian.Uint64(hi),
		Low:  binary.BigEndian.Uint64(lo),
	}
}

func (fb DBFlatBufferBuilder) addSortedRuns(sortedRuns []SortedRun) []*flatbuf.SortedRunT {
	sortedRunFBs := make([]*flatbuf.SortedRunT, 0)
	for _, sortedRun := range sortedRuns {
		sortedRunFBs = append(sortedRunFBs, fb.addSortedRun(sortedRun))
	}
	return sortedRunFBs
}

func (fb DBFlatBufferBuilder) addSortedRun(sortedRun SortedRun) *flatbuf.SortedRunT {
	return &flatbuf.SortedRunT{
		Id:   sortedRun.id,
		Ssts: fb.addCompactedSSTs(sortedRun.sstList),
	}
}

// ------------------------------------------------
// SSTableInfoOwned
// ------------------------------------------------

type SSTableInfoOwned struct {
	data []byte
}

func newSSTableInfoOwned(data []byte) *SSTableInfoOwned {
	return &SSTableInfoOwned{data: data}
}

func (info *SSTableInfoOwned) borrow() *flatbuf.SsTableInfo {
	return flatbuf.GetRootAsSsTableInfo(info.data, 0)
}

func (info *SSTableInfoOwned) clone() *SSTableInfoOwned {
	data := make([]byte, len(info.data))
	copy(data, info.data)
	return &SSTableInfoOwned{
		data: data,
	}
}

// ------------------------------------------------
// SSTableIndexOwned
// ------------------------------------------------

type SSTableIndexOwned struct {
	data []byte
}

func newSSTableIndexOwned(data []byte) *SSTableIndexOwned {
	return &SSTableIndexOwned{data: data}
}

func (info *SSTableIndexOwned) borrow() *flatbuf.SsTableIndex {
	return flatbuf.GetRootAsSsTableIndex(info.data, 0)
}

func (info *SSTableIndexOwned) clone() *SSTableIndexOwned {
	data := make([]byte, len(info.data))
	copy(data, info.data)
	return &SSTableIndexOwned{
		data: data,
	}
}

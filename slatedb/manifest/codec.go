package manifest

import (
	"bytes"
	"encoding/binary"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/flatbuf"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/compaction"
	"github.com/slatedb/slatedb-go/slatedb/state"
)

// ------------------------------------------------
// FlatBufferManifestCodec
// ------------------------------------------------

// FlatBufferManifestCodec implements Codec and defines how we
// Encode Manifest to byte slice and Decode byte slice back to Manifest
type FlatBufferManifestCodec struct{}

func (f FlatBufferManifestCodec) Encode(manifest *Manifest) []byte {
	builder := flatbuffers.NewBuilder(0)
	dbFlatBufBuilder := NewDBFlatBufferBuilder(builder)
	return dbFlatBufBuilder.createManifest(manifest)
}

func (f FlatBufferManifestCodec) Decode(data []byte) (*Manifest, error) {
	manifestV1 := flatbuf.GetRootAsManifestV1(data, 0)
	return f.manifest(manifestV1.UnPack()), nil
}

func (f FlatBufferManifestCodec) manifest(manifest *flatbuf.ManifestV1T) *Manifest {
	core := &state.CoreStateSnapshot{
		L0:        f.parseFlatBufSSTList(manifest.L0),
		Compacted: f.parseFlatBufSortedRuns(manifest.Compacted),
	}
	core.NextWalSstID.Store(manifest.WalIdLastSeen + 1)
	core.LastCompactedWalSSTID.Store(manifest.WalIdLastCompacted)

	l0LastCompacted := f.parseFlatBufSSTId(manifest.L0LastCompacted)
	if l0LastCompacted == ulid.Zero {
		core.L0LastCompacted = mo.None[ulid.ULID]()
	} else {
		core.L0LastCompacted = mo.Some(l0LastCompacted)
	}

	m := &Manifest{}
	m.Core = core.ToCoreState()
	m.WriterEpoch.Store(manifest.WriterEpoch)
	m.CompactorEpoch.Store(manifest.CompactorEpoch)
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

func (f FlatBufferManifestCodec) parseFlatBufSSTList(fbSSTList []*flatbuf.CompactedSsTableT) []sstable.Handle {
	sstList := make([]sstable.Handle, 0)
	for _, sst := range fbSSTList {
		id := f.parseFlatBufSSTId(sst.Id)
		sstList = append(sstList, sstable.Handle{
			Id:   sstable.NewIDCompacted(id),
			Info: f.parseFlatBufSSTInfo(sst.Info),
		})
	}
	return sstList
}

func (f FlatBufferManifestCodec) parseFlatBufSSTInfo(info *flatbuf.SsTableInfoT) *sstable.Info {
	return &sstable.Info{
		FirstKey:         bytes.Clone(info.FirstKey),
		IndexOffset:      info.IndexOffset,
		IndexLen:         info.IndexLen,
		FilterOffset:     info.FilterOffset,
		FilterLen:        info.FilterLen,
		CompressionCodec: compress.CodecFromFlatBuf(info.CompressionFormat),
	}
}

func (f FlatBufferManifestCodec) parseFlatBufSortedRuns(fbSortedRuns []*flatbuf.SortedRunT) []compaction.SortedRun {
	sortedRuns := make([]compaction.SortedRun, 0)
	for _, run := range fbSortedRuns {
		sortedRuns = append(sortedRuns, compaction.SortedRun{
			ID:      run.Id,
			SSTList: f.parseFlatBufSSTList(run.Ssts),
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

func NewDBFlatBufferBuilder(builder *flatbuffers.Builder) DBFlatBufferBuilder {
	return DBFlatBufferBuilder{builder}
}

func (fb *DBFlatBufferBuilder) createManifest(manifest *Manifest) []byte {
	core := manifest.Core.Snapshot()
	l0 := fb.sstListToFlatBuf(core.L0)
	var l0LastCompacted *flatbuf.CompactedSstIdT
	if core.L0LastCompacted.IsPresent() {
		id, _ := core.L0LastCompacted.Get()
		l0LastCompacted = fb.compactedSSTID(id)
	}
	compacted := fb.sortedRunsToFlatBuf(core.Compacted)

	manifestV1 := flatbuf.ManifestV1T{
		ManifestId:         0,
		WriterEpoch:        manifest.WriterEpoch.Load(),
		CompactorEpoch:     manifest.CompactorEpoch.Load(),
		WalIdLastCompacted: core.LastCompactedWalSSTID.Load(),
		WalIdLastSeen:      core.NextWalSstID.Load() - 1,
		L0LastCompacted:    l0LastCompacted,
		L0:                 l0,
		Compacted:          compacted,
		Snapshots:          nil,
	}
	manifestOffset := manifestV1.Pack(fb.builder)
	fb.builder.Finish(manifestOffset)
	return fb.builder.FinishedBytes()
}

func (fb *DBFlatBufferBuilder) sstListToFlatBuf(sstList []sstable.Handle) []*flatbuf.CompactedSsTableT {
	compactedSSTs := make([]*flatbuf.CompactedSsTableT, 0)
	for _, sst := range sstList {
		compactedSSTs = append(compactedSSTs, fb.compactedSST(sst.Id, sst.Info))
	}
	return compactedSSTs
}

func (fb *DBFlatBufferBuilder) compactedSST(sstID sstable.ID, sstInfo *sstable.Info) *flatbuf.CompactedSsTableT {
	assert.True(sstID.Type == sstable.Compacted, "cannot pass WAL SST handle to create compacted sst")
	id, err := ulid.Parse(sstID.Value)
	if err != nil {
		return nil
	}

	return &flatbuf.CompactedSsTableT{
		Id:   fb.compactedSSTID(id),
		Info: sstable.SstInfoToFlatBuf(sstInfo),
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

func (fb *DBFlatBufferBuilder) sortedRunsToFlatBuf(sortedRuns []compaction.SortedRun) []*flatbuf.SortedRunT {
	sortedRunFBs := make([]*flatbuf.SortedRunT, 0)
	for _, sortedRun := range sortedRuns {
		sortedRunFBs = append(sortedRunFBs, &flatbuf.SortedRunT{
			Id:   sortedRun.ID,
			Ssts: fb.sstListToFlatBuf(sortedRun.SSTList),
		})
	}
	return sortedRunFBs
}

package slatedb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/slatedb/slatedb-go/internal/assert"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/maypok86/otter"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/sstable/bloom"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/thanos-io/objstore"
)

// ------------------------------------------------
// TableStore is an abstraction over object storage
// to read/write SSTable data
// ------------------------------------------------

type TableStore struct {
	mu            sync.RWMutex
	bucket        objstore.Bucket
	sstConfig     sstable.Config
	rootPath      string
	walPath       string
	compactedPath string
	filterCache   otter.Cache[sstable.ID, mo.Option[bloom.Filter]]
}

func NewTableStore(bucket objstore.Bucket, sstConfig sstable.Config, rootPath string) *TableStore {
	cache, err := otter.MustBuilder[sstable.ID, mo.Option[bloom.Filter]](1000).Build()
	assert.True(err == nil, "")
	return &TableStore{
		bucket:        bucket,
		sstConfig:     sstConfig,
		rootPath:      rootPath,
		walPath:       "wal",
		compactedPath: "compacted",
		filterCache:   cache,
	}
}

// Get list of WALs from object store that are not compacted (walID greater than walIDLastCompacted)
func (ts *TableStore) getWalSSTList(walIDLastCompacted uint64) ([]uint64, error) {
	walList := make([]uint64, 0)
	walPath := path.Join(ts.rootPath, ts.walPath)

	err := ts.bucket.Iter(context.Background(), walPath, func(filepath string) error {
		if strings.Contains(filepath, ".sst") {
			walID, err := ts.parseID(filepath, ".sst")
			if err == nil && walID > walIDLastCompacted {
				walList = append(walList, walID)
			}
		}
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return nil, fmt.Errorf("while iterating over the table list: %w", err)
	}

	slices.Sort(walList)
	return walList, nil
}

func (ts *TableStore) TableWriter(sstID sstable.ID) *EncodedSSTableWriter {
	return &EncodedSSTableWriter{
		builder:       sstable.NewBuilder(ts.sstConfig),
		sstID:         sstID,
		tableStore:    ts,
		blocksWritten: 0,
	}
}

func (ts *TableStore) TableBuilder() *sstable.Builder {
	return sstable.NewBuilder(ts.sstConfig)
}

func (ts *TableStore) WriteSST(id sstable.ID, encodedSST *sstable.Table) (*sstable.Handle, error) {
	sstPath := ts.sstPath(id)

	blocksData := make([]byte, 0)
	for i := 0; i < encodedSST.Blocks.Len(); i++ {
		blocksData = append(blocksData, encodedSST.Blocks.At(i)...)
	}

	err := ts.bucket.Upload(context.Background(), sstPath, bytes.NewReader(blocksData))
	if err != nil {
		return nil, fmt.Errorf("during object write: %w", err)
	}

	ts.cacheFilter(id, encodedSST.Bloom)
	return sstable.NewHandle(id, encodedSST.Info), nil
}

func (ts *TableStore) OpenSST(id sstable.ID) (*sstable.Handle, error) {
	obj := ReadOnlyObject{ts.bucket, ts.sstPath(id)}
	sstInfo, err := sstable.ReadInfo(obj)
	if err != nil {
		return nil, fmt.Errorf("while reading sst info: %w", err)
	}

	return sstable.NewHandle(id, sstInfo), nil
}

func (ts *TableStore) ReadBlocks(sstHandle *sstable.Handle, blocksRange common.Range) ([]block.Block, error) {
	obj := ReadOnlyObject{ts.bucket, ts.sstPath(sstHandle.Id)}
	index, err := sstable.ReadIndex(sstHandle.Info, obj)
	if err != nil {
		return nil, err
	}
	return sstable.ReadBlocks(sstHandle.Info, index, blocksRange, obj)
}

// Reads specified blocks from an SSTable using the provided index.
func (ts *TableStore) ReadBlocksUsingIndex(
	sstHandle *sstable.Handle,
	blocksRange common.Range,
	index *sstable.Index,
) ([]block.Block, error) {
	obj := ReadOnlyObject{ts.bucket, ts.sstPath(sstHandle.Id)}
	return sstable.ReadBlocks(sstHandle.Info, index, blocksRange, obj)
}

func (ts *TableStore) cacheFilter(sstID sstable.ID, filter mo.Option[bloom.Filter]) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.filterCache.Set(sstID, filter)
}

func (ts *TableStore) ReadFilter(sstHandle *sstable.Handle) (mo.Option[bloom.Filter], error) {
	ts.mu.RLock()
	val, ok := ts.filterCache.Get(sstHandle.Id)
	ts.mu.RUnlock()
	if ok {
		return val, nil
	}

	obj := ReadOnlyObject{ts.bucket, ts.sstPath(sstHandle.Id)}
	filtr, err := sstable.ReadFilter(sstHandle.Info, obj)
	if err != nil {
		return mo.None[bloom.Filter](), err
	}

	ts.cacheFilter(sstHandle.Id, filtr)
	return filtr, nil
}

func (ts *TableStore) ReadIndex(sstHandle *sstable.Handle) (*sstable.Index, error) {
	obj := ReadOnlyObject{ts.bucket, ts.sstPath(sstHandle.Id)}
	index, err := sstable.ReadIndex(sstHandle.Info, obj)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func (ts *TableStore) sstPath(id sstable.ID) string {
	if id.Type == sstable.WAL {
		return path.Join(ts.rootPath, ts.walPath, id.Value+".sst")
	} else if id.Type == sstable.Compacted {
		return path.Join(ts.rootPath, ts.compactedPath, id.Value+".sst")
	}
	return ""
}

func (ts *TableStore) parseID(filepath string, expectedExt string) (uint64, error) {
	assert.True(path.Ext(filepath) == expectedExt, "invalid wal file")

	base := path.Base(filepath)
	idStr := strings.Replace(base, expectedExt, "", 1)
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("while parsing id '%s': %w", idStr, err)
	}

	return id, nil
}

func (ts *TableStore) Clone() *TableStore {
	cache, err := otter.MustBuilder[sstable.ID, mo.Option[bloom.Filter]](1000).Build()
	assert.True(err == nil, "")
	return &TableStore{
		mu:            sync.RWMutex{},
		bucket:        ts.bucket,
		sstConfig:     ts.sstConfig,
		rootPath:      ts.rootPath,
		walPath:       ts.walPath,
		compactedPath: ts.compactedPath,
		filterCache:   cache,
	}
}

// ------------------------------------------------
// EncodedSSTableWriter
// Thrawn01: (Only Used By The Compactor)
// ------------------------------------------------

type EncodedSSTableWriter struct {
	sstID      sstable.ID
	builder    *sstable.Builder
	tableStore *TableStore

	// TODO: we are using an unbounded slice of byte as buffer.
	//  Add a capacity for buffer and when buffer reaches the capacity
	//  it should be written to object storage
	buffer        []byte
	blocksWritten uint64
}

func (w *EncodedSSTableWriter) Add(key []byte, value mo.Option[[]byte]) error {
	v, _ := value.Get()
	err := w.builder.AddValue(key, v)
	if err != nil {
		return fmt.Errorf("builder failed to add key value: %w", err)
	}

	for {
		blk, ok := w.builder.NextBlock().Get()
		if !ok {
			break
		}
		w.buffer = append(w.buffer, blk...)
		w.blocksWritten += 1
	}

	return nil
}

func (w *EncodedSSTableWriter) Written() uint64 {
	return w.blocksWritten
}

func (w *EncodedSSTableWriter) Close() (*sstable.Handle, error) {
	encodedSST, err := w.builder.Build()
	if err != nil {
		return nil, fmt.Errorf("SST build failed: %w", err)
	}

	blocksData := w.buffer
	for {
		if encodedSST.Blocks.Len() == 0 {
			break
		}
		blocksData = append(blocksData, encodedSST.Blocks.PopFront()...)
	}

	sstPath := w.tableStore.sstPath(w.sstID)
	err = w.tableStore.bucket.Upload(context.Background(), sstPath, bytes.NewReader(blocksData))
	if err != nil {
		return nil, common.ErrObjectStore
	}

	w.tableStore.cacheFilter(w.sstID, encodedSST.Bloom)
	return sstable.NewHandle(w.sstID, encodedSST.Info), nil
}

// ------------------------------------------------
// ReadOnlyObject
// ------------------------------------------------

type ReadOnlyObject struct {
	bucket objstore.Bucket
	path   string
}

func (r ReadOnlyObject) Len() (int, error) {
	attr, err := r.bucket.Attributes(context.Background(), r.path)
	if err != nil {
		return 0, fmt.Errorf("while fetching object attributes: %w", err)
	}
	return int(attr.Size), nil
}

func (r ReadOnlyObject) ReadRange(rng common.Range) ([]byte, error) {
	read, err := r.bucket.GetRange(context.Background(), r.path, int64(rng.Start), int64(rng.End-rng.Start))
	if err != nil {
		return nil, fmt.Errorf("while fetching object range [%d:%d]: %w", rng.Start, rng.End-rng.Start, err)
	}

	data, err := io.ReadAll(read)
	if err != nil {
		return nil, fmt.Errorf("while reading object [%d:%d]: %w", rng.Start, rng.End, err)
	}

	return data, nil
}

func (r ReadOnlyObject) Read() ([]byte, error) {
	read, err := r.bucket.Get(context.Background(), r.path)
	if err != nil {
		return nil, fmt.Errorf("while fetching object '%s': %w", r.path, err)
	}

	data, err := io.ReadAll(read)
	if err != nil {
		return nil, fmt.Errorf("while reading object '%s': %w", r.path, err)
	}

	return data, nil
}

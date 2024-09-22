package slatedb

import (
	"bytes"
	"context"
	"github.com/maypok86/otter"
	"github.com/samber/mo"
	"github.com/thanos-io/objstore"
	"io"
	"log"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// ------------------------------------------------
// TableStore
// ------------------------------------------------

type TableStore struct {
	mu            sync.RWMutex
	bucket        objstore.Bucket
	sstFormat     *SSTableFormat
	rootPath      string
	walPath       string
	compactedPath string
	filterCache   otter.Cache[SSTableID, mo.Option[BloomFilter]]
}

func newTableStore(bucket objstore.Bucket, format *SSTableFormat, rootPath string) *TableStore {
	cache, err := otter.MustBuilder[SSTableID, mo.Option[BloomFilter]](1000).Build()
	assertTrue(err == nil, "")
	return &TableStore{
		bucket:        bucket,
		sstFormat:     format,
		rootPath:      rootPath,
		walPath:       "wal",
		compactedPath: "compacted",
		filterCache:   cache,
	}
}

func (ts *TableStore) getWalSSTList(walIDLastCompacted uint64) ([]uint64, error) {
	walList := make([]uint64, 0)
	walPath := path.Join(ts.rootPath, ts.walPath)

	ts.bucket.Iter(context.Background(), walPath, func(filepath string) error {
		if strings.Contains(filepath, ".sst") {
			walID, err := ts.parseID(filepath, ".sst")
			if err == nil && walID > walIDLastCompacted {
				walList = append(walList, walID)
			}
		}
		return nil
	})

	slices.Sort(walList)
	return walList, nil
}

func (ts *TableStore) tableWriter(sstID SSTableID) *EncodedSSTableWriter {
	sstPath := ts.sstPath(sstID)
	err := ts.bucket.Upload(context.Background(), sstPath, bytes.NewReader([]byte{}))
	if err != nil {
		log.Fatal(err)
	}

	return &EncodedSSTableWriter{
		sstID:         sstID,
		builder:       ts.sstFormat.tableBuilder(),
		tableStore:    ts,
		blocksWritten: 0,
	}
}

func (ts *TableStore) tableBuilder() *EncodedSSTableBuilder {
	return ts.sstFormat.tableBuilder()
}

func (ts *TableStore) writeSST(id SSTableID, encodedSST *EncodedSSTable) (*SSTableHandle, error) {
	sstPath := ts.sstPath(id)

	blocksData := make([]byte, 0)
	for i := 0; i < encodedSST.unconsumedBlocks.Len(); i++ {
		blocksData = append(blocksData, encodedSST.unconsumedBlocks.At(i)...)
	}

	err := ts.bucket.Upload(context.Background(), sstPath, bytes.NewReader(blocksData))
	if err != nil {
		return nil, ErrObjectStore
	}

	ts.cacheFilter(id, encodedSST.filter)
	return newSSTableHandle(id, encodedSST.sstInfo), nil
}

func (ts *TableStore) openSST(id SSTableID) (*SSTableHandle, error) {
	obj := ReadOnlyObject{ts.bucket, ts.sstPath(id)}
	sstInfo, err := ts.sstFormat.readInfo(obj)
	if err != nil {
		return nil, err
	}

	return newSSTableHandle(id, sstInfo), nil
}

func (ts *TableStore) readBlocks(sstHandle *SSTableHandle, blocksRange Range) ([]Block, error) {
	obj := ReadOnlyObject{ts.bucket, ts.sstPath(sstHandle.id)}
	return ts.sstFormat.readBlocks(sstHandle.info, blocksRange, obj)
}

func (ts *TableStore) cacheFilter(sstID SSTableID, filter mo.Option[BloomFilter]) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.filterCache.Set(sstID, filter)
}

func (ts *TableStore) readFilter(sstHandle *SSTableHandle) (mo.Option[BloomFilter], error) {
	ts.mu.RLock()
	val, ok := ts.filterCache.Get(sstHandle.id)
	ts.mu.RUnlock()
	if ok {
		return val, nil
	}

	obj := ReadOnlyObject{ts.bucket, ts.sstPath(sstHandle.id)}
	filter, err := ts.sstFormat.readFilter(sstHandle.info, obj)
	if err != nil {
		return mo.None[BloomFilter](), err
	}

	ts.cacheFilter(sstHandle.id, filter)
	return filter, nil
}

func (ts *TableStore) sstPath(id SSTableID) string {
	if id.typ == WAL {
		return path.Join(ts.rootPath, ts.walPath, id.value+".sst")
	} else if id.typ == Compacted {
		return path.Join(ts.rootPath, ts.compactedPath, id.value+".sst")
	}
	return ""
}

func (ts *TableStore) parseID(filepath string, expectedExt string) (uint64, error) {
	assertTrue(path.Ext(filepath) == expectedExt, "invalid wal file")

	name := path.Base(filepath)
	id, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, ErrInvalidDBState
	}

	return id, nil
}

// ------------------------------------------------
// EncodedSSTableWriter
// ------------------------------------------------

type EncodedSSTableWriter struct {
	sstID         SSTableID
	builder       *EncodedSSTableBuilder
	tableStore    *TableStore
	blocksWritten uint64
}

func (w *EncodedSSTableWriter) add(key []byte, value mo.Option[[]byte]) error {
	err := w.builder.add(key, value)
	if err != nil {
		return err
	}

	blocksData := make([]byte, 0)
	for {
		block, ok := w.builder.nextBlock().Get()
		if !ok {
			break
		}
		blocksData = append(blocksData, block...)
		w.blocksWritten += 1
	}

	sstPath := w.tableStore.sstPath(w.sstID)
	err = w.tableStore.bucket.Upload(context.Background(), sstPath, bytes.NewReader(blocksData))
	if err != nil {
		return ErrObjectStore
	}
	return nil
}

func (w *EncodedSSTableWriter) close() (*SSTableHandle, error) {
	encodedSST, err := w.builder.build()
	if err != nil {
		return nil, err
	}

	blocksData := make([]byte, 0)
	for {
		if encodedSST.unconsumedBlocks.Len() == 0 {
			break
		}
		blocksData = append(blocksData, encodedSST.unconsumedBlocks.PopFront()...)
	}

	sstPath := w.tableStore.sstPath(w.sstID)
	err = w.tableStore.bucket.Upload(context.Background(), sstPath, bytes.NewReader(blocksData))
	if err != nil {
		return nil, ErrObjectStore
	}

	w.tableStore.cacheFilter(w.sstID, encodedSST.filter)
	return newSSTableHandle(w.sstID, encodedSST.sstInfo), nil
}

// ------------------------------------------------
// ReadOnlyObject
// ------------------------------------------------

type ReadOnlyObject struct {
	bucket objstore.Bucket
	path   string
}

func (r ReadOnlyObject) len() (int, error) {
	attr, err := r.bucket.Attributes(context.Background(), r.path)
	if err != nil {
		return 0, ErrObjectStore
	}
	return int(attr.Size), nil
}

func (r ReadOnlyObject) readRange(rng Range) ([]byte, error) {
	read, err := r.bucket.GetRange(context.Background(), r.path, int64(rng.start), int64(rng.end-rng.start))
	if err != nil {
		return nil, ErrObjectStore
	}

	data, err := io.ReadAll(read)
	if err != nil {
		return nil, ErrObjectStore
	}

	return data, nil
}

func (r ReadOnlyObject) read() ([]byte, error) {
	read, err := r.bucket.Get(context.Background(), r.path)
	if err != nil {
		return nil, ErrObjectStore
	}

	data, err := io.ReadAll(read)
	if err != nil {
		return nil, ErrObjectStore
	}

	return data, nil
}

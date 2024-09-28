package slatedb

import "sync"

type Compactor struct {
	sync.Mutex
}

func newCompactor(manifestStore *ManifestStore, tableStore *TableStore, options *CompactorOptions) (*Compactor, error) {
	// TODO: implement
	return nil, nil
}

func (c *Compactor) close() {}

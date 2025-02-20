package sstable

import (
	"context"
	"github.com/slatedb/slatedb-go/internal"

	"github.com/slatedb/slatedb-go/slatedb/common"
)

// NewBytesBlob creates a ReadOnlyBlob from a byte slice for testing purposes
func NewBytesBlob(data []byte) common.ReadOnlyBlob {
	return &bytesBlob{data: data}
}

type bytesBlob struct {
	data []byte
}

func (b *bytesBlob) Len(_ context.Context) (int, error) {
	return len(b.data), nil
}

func (b *bytesBlob) ReadRange(_ context.Context, r common.Range) ([]byte, error) {
	if r.Start > uint64(len(b.data)) || r.End > uint64(len(b.data)) || r.Start > r.End {
		return nil, internal.Err("corrupted; [%d:%d] is an invalid range", r.Start, r.End)
	}
	return b.data[r.Start:r.End], nil
}

func (b *bytesBlob) Read(_ context.Context) ([]byte, error) {
	return b.data, nil
}

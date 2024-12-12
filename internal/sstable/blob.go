package sstable

import (
	"errors"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

// NewBytesBlob creates a ReadOnlyBlob from a byte slice for testing purposes
func NewBytesBlob(data []byte) common.ReadOnlyBlob {
	return &bytesBlob{data: data}
}

type bytesBlob struct {
	data []byte
}

func (b *bytesBlob) Len() (int, error) {
	return len(b.data), nil
}

func (b *bytesBlob) ReadRange(r common.Range) ([]byte, error) {
	if r.Start > uint64(len(b.data)) || r.End > uint64(len(b.data)) || r.Start > r.End {
		return nil, errors.New("invalid range")
	}
	return b.data[r.Start:r.End], nil
}

func (b *bytesBlob) Read() ([]byte, error) {
	return b.data, nil
}

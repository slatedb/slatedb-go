package common

import "context"

type ReadOnlyBlob interface {
	Len(ctx context.Context) (int, error)
	ReadRange(ctx context.Context, r Range) ([]byte, error)
	Read(ctx context.Context) ([]byte, error)
}

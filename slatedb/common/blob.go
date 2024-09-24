package common

type ReadOnlyBlob interface {
	Len() (int, error)
	ReadRange(r Range) ([]byte, error)
	Read() ([]byte, error)
}

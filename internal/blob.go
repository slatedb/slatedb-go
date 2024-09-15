package internal

type ReadOnlyBlob interface {
	len() (int, error)
	readRange(r Range) ([]byte, error)
	read() ([]byte, error)
}

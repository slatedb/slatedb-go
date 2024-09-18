package slatedb

import (
	"fmt"
	"github.com/oklog/ulid/v2"
)

type SSTableIdType int

const (
	WAL SSTableIdType = iota
	Compacted
)

type SSTableId struct {
	typ   SSTableIdType
	value string
}

func ssTableIdWal(id uint64) SSTableId {
	return SSTableId{typ: WAL, value: fmt.Sprintf("%020d", id)}
}

func ssTableIdCompacted(id ulid.ULID) SSTableId {
	return SSTableId{typ: Compacted, value: id.String()}
}

type SSTableHandle struct {
	id   SSTableId
	info *SSTableInfoOwned
}

type CoreDbState struct {
}

package sstable

import (
	"bytes"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"log/slog"
	"strconv"
)

type IDType int

const (
	WAL IDType = iota + 1
	Compacted
)

type ID struct {
	Type  IDType
	Value string
}

func NewIDWal(id uint64) ID {
	return ID{Type: WAL, Value: fmt.Sprintf("%020d", id)}
}

func NewIDCompacted(id ulid.ULID) ID {
	return ID{Type: Compacted, Value: id.String()}
}

// TODO(thrawn01): If ID can represent both a ulid and uint64 then we
//  need to handle the error here, instead of just logging the error.
func (s *ID) WalID() mo.Option[uint64] {
	if s.Type != WAL {
		return mo.None[uint64]()
	}

	val, err := strconv.Atoi(s.Value)
	if err != nil {
		slog.Error("unable to parse table id", "error", err)
		return mo.None[uint64]()
	}

	return mo.Some(uint64(val))
}

func (s *ID) CompactedID() mo.Option[ulid.ULID] {
	if s.Type != Compacted {
		return mo.None[ulid.ULID]()
	}

	val, err := ulid.Parse(s.Value)
	if err != nil {
		slog.Error("unable to parse table id", "error", err)
		return mo.None[ulid.ULID]()
	}

	return mo.Some(val)
}

func (s *ID) Clone() ID {
	var sstID ID
	if s.Type == WAL {
		id, _ := s.WalID().Get()
		sstID = NewIDWal(id)
	} else if s.Type == Compacted {
		id, _ := s.CompactedID().Get()
		sstID = NewIDCompacted(id)
	}
	return sstID
}

// Handle represents the SSTable
// TODO(thrawn01): I think this should merge with sstable.Table
type Handle struct {
	Id   ID
	Info *Info
}

func NewHandle(id ID, info *Info) *Handle {
	return &Handle{id, info}
}

func (h *Handle) RangeCoversKey(key []byte) bool {
	if len(h.Info.FirstKey) == 0 {
		return false
	}
	return bytes.Compare(key, h.Info.FirstKey) >= 0
}

func (h *Handle) Clone() *Handle {
	return &Handle{
		Id:   h.Id.Clone(),
		Info: h.Info.Clone(),
	}
}

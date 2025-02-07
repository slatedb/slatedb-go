package compaction

import (
	"context"
	"strconv"

	"github.com/oklog/ulid/v2"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/compacted"
	"github.com/slatedb/slatedb-go/slatedb/config"
	"github.com/slatedb/slatedb-go/slatedb/store"
)

type Scheduler interface {
	maybeScheduleCompaction(state *CompactorState) []Compaction
}

type CompactorMainMsg int

const (
	CompactorShutdown CompactorMainMsg = iota + 1
)

type Result struct {
	SortedRun *compacted.SortedRun
	Error     error
}

// ------------------------------------------------
// SourceID
// ------------------------------------------------

type SourceIDType int

const (
	SortedRunID SourceIDType = iota + 1
	SSTID
)

type SourceID struct {
	typ   SourceIDType
	value string
}

func NewSourceIDSST(id ulid.ULID) SourceID {
	return SourceID{
		typ:   SSTID,
		value: id.String(),
	}
}

func (s SourceID) SortedRunID() mo.Option[uint32] {
	if s.typ != SortedRunID {
		return mo.None[uint32]()
	}
	val, err := strconv.Atoi(s.value)
	if err != nil {
		return mo.None[uint32]()
	}
	return mo.Some(uint32(val))
}

func (s SourceID) SstID() mo.Option[ulid.ULID] {
	if s.typ != SSTID {
		return mo.None[ulid.ULID]()
	}
	val, err := ulid.Parse(s.value)
	if err != nil {
		return mo.None[ulid.ULID]()
	}
	return mo.Some(val)
}

// ------------------------------------------------
// Compaction
// ------------------------------------------------

type Status int

const (
	Submitted Status = iota + 1
	InProgress
)

type Compaction struct {
	Status      Status
	sources     []SourceID
	destination uint32
}

func NewCompaction(sources []SourceID, destination uint32) Compaction {
	return Compaction{
		Status:      Submitted,
		sources:     sources,
		destination: destination,
	}
}

// Compactor - The Orchestrator checks with the Scheduler if Level0 needs to be compacted.
// If compaction is needed, the Orchestrator gives Jobs to the Executor.
// The Executor creates new goroutine for each Job and the results are written to a channel.
type Compactor struct {
	orchestrator *Orchestrator
}

func NewCompactor(manifestStore *store.ManifestStore, tableStore *store.TableStore, opts config.DBOptions) (*Compactor, error) {
	orchestrator, err := spawnAndRunCompactionOrchestrator(manifestStore, tableStore, opts)
	if err != nil {
		return nil, err
	}

	return &Compactor{
		orchestrator: orchestrator,
	}, nil
}

func (c *Compactor) Close(ctx context.Context) error {
	return c.orchestrator.shutdown(ctx)
}

func spawnAndRunCompactionOrchestrator(
	manifestStore *store.ManifestStore,
	tableStore *store.TableStore,
	opts config.DBOptions,
) (*Orchestrator, error) {
	orchestrator, err := NewOrchestrator(opts, manifestStore, tableStore)
	if err != nil {
		return nil, err
	}

	orchestrator.spawnLoop(opts)
	return orchestrator, nil
}

type Job struct {
	destination uint32
	sstList     []sstable.Handle
	sortedRuns  []compacted.SortedRun
}

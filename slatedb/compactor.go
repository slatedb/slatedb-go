package slatedb

import (
	"context"
	"errors"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	compaction2 "github.com/slatedb/slatedb-go/slatedb/compaction"
	"github.com/slatedb/slatedb-go/slatedb/store"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/slatedb/slatedb-go/internal/iter"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

type CompactionScheduler interface {
	maybeScheduleCompaction(state *CompactorState) []Compaction
}

type CompactorMainMsg int

const (
	CompactorShutdown CompactorMainMsg = iota + 1
)

type WorkerToOrchestratorMsg struct {
	CompactionResult *compaction2.SortedRun
	CompactionError  error
}

// Compactor creates one goroutine for CompactorOrchestrator. The CompactorOrchestrator can create multiple goroutines
// for running the actual compaction. The result of the compaction are shared back to the CompactorOrchestrator through
// CompactorOrchestrator.workerCh channel.
// When Compactor.close is called, we wait till all the goroutines/workers started by CompactorOrchestrator stop running
type Compactor struct {
	// compactorMsgCh - When Compactor.close is called we write CompactorShutdown message to this channel
	// CompactorOrchestrator which runs in a different goroutine reads this channel and shuts down
	compactorMsgCh chan<- CompactorMainMsg

	// compactorWG - When Compactor.close is called then wait till goroutine running CompactorOrchestrator
	// is completed
	compactorWG *sync.WaitGroup
}

func newCompactor(manifestStore *ManifestStore, tableStore *store.TableStore, opts DBOptions) (*Compactor, error) {
	compactorMsgCh := make(chan CompactorMainMsg, math.MaxUint8)

	compactorWG, errCh := spawnAndRunCompactorOrchestrator(manifestStore, tableStore, opts, compactorMsgCh)

	err := <-errCh
	assert.True(err == nil, "Failed to start compactor")

	return &Compactor{
		compactorMsgCh: compactorMsgCh,
		compactorWG:    compactorWG,
	}, nil
}

func (c *Compactor) close() {
	c.compactorMsgCh <- CompactorShutdown
	c.compactorWG.Wait()
}

// ------------------------------------------------
// CompactorOrchestrator
// ------------------------------------------------

func spawnAndRunCompactorOrchestrator(
	manifestStore *ManifestStore,
	tableStore *store.TableStore,
	opts DBOptions,
	compactorMsgCh <-chan CompactorMainMsg,
) (*sync.WaitGroup, chan error) {

	errCh := make(chan error)
	compactorWG := &sync.WaitGroup{}
	compactorWG.Add(1)

	go func() {
		defer compactorWG.Done()
		orchestrator, err := newCompactorOrchestrator(opts, manifestStore, tableStore, compactorMsgCh)
		if err != nil {
			errCh <- err
			return
		}
		errCh <- nil

		ticker := time.NewTicker(opts.CompactorOptions.PollInterval)
		defer ticker.Stop()

		// TODO(thrawn01): Race, cannot know if no more work is coming by checking length of the channel
		for !(orchestrator.executor.isStopped() && len(orchestrator.workerCh) == 0) {
			select {
			case <-ticker.C:
				err := orchestrator.loadManifest()
				assert.True(err == nil, "Failed to load manifest")
			case msg := <-orchestrator.workerCh:
				if msg.CompactionError != nil {
					opts.Log.Error("Error executing compaction", "error", msg.CompactionError)
				} else if msg.CompactionResult != nil {
					err := orchestrator.finishCompaction(msg.CompactionResult)
					assert.True(err == nil, "Failed to finish compaction")
				}
			case <-orchestrator.compactorMsgCh:
				// we receive Shutdown msg on compactorMsgCh.
				// Stop the executor. Don't return because there might
				// still be messages in `orchestrator.workerCh`. Let the loop continue
				// to drain them until empty.
				orchestrator.executor.stop()
				ticker.Stop()
			}
		}
	}()

	return compactorWG, errCh
}

type CompactorOrchestrator struct {
	options   *CompactorOptions
	manifest  *FenceableManifest
	state     *CompactorState
	scheduler CompactionScheduler
	executor  *CompactionExecutor

	// compactorMsgCh - When CompactorOrchestrator receives a CompactorShutdown message on this channel,
	// it calls executor.stop
	compactorMsgCh <-chan CompactorMainMsg

	// workerCh - CompactionExecutor sends a CompactionFinished message to this channel once compaction is done.
	// The CompactorOrchestrator loops through each item in this channel and calls finishCompaction
	workerCh <-chan WorkerToOrchestratorMsg
	log      *slog.Logger
}

func newCompactorOrchestrator(
	opts DBOptions,
	manifestStore *ManifestStore,
	tableStore *store.TableStore,
	compactorMsgCh <-chan CompactorMainMsg,
) (*CompactorOrchestrator, error) {
	sm, err := loadStoredManifest(manifestStore)
	if err != nil {
		return nil, err
	}
	if sm.IsAbsent() {
		return nil, common.ErrInvalidDBState
	}
	storedManifest, _ := sm.Get()

	manifest, err := initFenceableManifestCompactor(&storedManifest)
	if err != nil {
		return nil, err
	}

	state, err := loadState(manifest)
	if err != nil {
		return nil, err
	}

	scheduler := loadCompactionScheduler()
	workerCh := make(chan WorkerToOrchestratorMsg, 1)
	executor := newCompactorExecutor(opts.CompactorOptions, workerCh, tableStore)

	o := CompactorOrchestrator{
		options:        opts.CompactorOptions,
		manifest:       manifest,
		state:          state,
		scheduler:      scheduler,
		executor:       executor,
		compactorMsgCh: compactorMsgCh,
		workerCh:       workerCh,
		log:            opts.Log,
	}
	return &o, nil
}

func loadState(manifest *FenceableManifest) (*CompactorState, error) {
	dbState, err := manifest.dbState()
	if err != nil {
		return nil, err
	}
	return newCompactorState(dbState.Clone(), nil), nil
}

func loadCompactionScheduler() CompactionScheduler {
	return SizeTieredCompactionScheduler{}
}

func (o *CompactorOrchestrator) loadManifest() error {
	_, err := o.manifest.refresh()
	if err != nil {
		return err
	}
	err = o.refreshDBState()
	if err != nil {
		return err
	}
	return nil
}

func (o *CompactorOrchestrator) refreshDBState() error {
	state, err := o.manifest.dbState()
	if err != nil {
		return err
	}

	o.state.refreshDBState(state)
	err = o.maybeScheduleCompactions()
	if err != nil {
		return err
	}
	return nil
}

func (o *CompactorOrchestrator) maybeScheduleCompactions() error {
	compactions := o.scheduler.maybeScheduleCompaction(o.state)
	for _, compaction := range compactions {
		err := o.submitCompaction(compaction)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *CompactorOrchestrator) startCompaction(compaction Compaction) {
	o.logCompactionState()
	dbState := o.state.dbState

	sstsByID := make(map[ulid.ULID]sstable.Handle)
	for _, sst := range dbState.L0 {
		id, ok := sst.Id.CompactedID().Get()
		assert.True(ok, "expected valid compacted ID")
		sstsByID[id] = sst
	}
	for _, sr := range dbState.Compacted {
		for _, sst := range sr.SSTList {
			id, ok := sst.Id.CompactedID().Get()
			assert.True(ok, "expected valid compacted ID")
			sstsByID[id] = sst
		}
	}

	srsByID := make(map[uint32]compaction2.SortedRun)
	for _, sr := range dbState.Compacted {
		srsByID[sr.ID] = sr
	}

	ssts := make([]sstable.Handle, 0)
	for _, sID := range compaction.sources {
		sstID, ok := sID.sstID().Get()
		if ok {
			ssts = append(ssts, sstsByID[sstID])
		}
	}

	sortedRuns := make([]compaction2.SortedRun, 0)
	for _, sID := range compaction.sources {
		srID, ok := sID.sortedRunID().Get()
		if ok {
			sortedRuns = append(sortedRuns, srsByID[srID])
		}
	}

	o.executor.startCompaction(CompactionJob{
		destination: compaction.destination,
		sstList:     ssts,
		sortedRuns:  sortedRuns,
	})
}

func (o *CompactorOrchestrator) finishCompaction(outputSR *compaction2.SortedRun) error {
	o.state.finishCompaction(outputSR)
	o.logCompactionState()
	err := o.writeManifest()
	if err != nil {
		return err
	}

	err = o.maybeScheduleCompactions()
	if err != nil {
		return err
	}
	return nil
}

func (o *CompactorOrchestrator) writeManifest() error {
	for {
		err := o.loadManifest()
		if err != nil {
			return err
		}

		core := o.state.dbState.Clone()
		err = o.manifest.updateDBState(core)
		if errors.Is(err, common.ErrManifestVersionExists) {
			o.log.Warn("conflicting manifest version. retry write", "error", err)
			continue
		}
		return err
	}
}

func (o *CompactorOrchestrator) submitCompaction(compaction Compaction) error {
	err := o.state.submitCompaction(compaction)
	if err != nil {
		o.log.Warn("invalid compaction", "error", err)
		return nil
	}
	o.startCompaction(compaction)
	return nil
}

func (o *CompactorOrchestrator) logCompactionState() {
	// LogState(o.log, o.state.dbState)
	for _, compaction := range o.state.compactions {
		o.log.Info("in-flight compaction", "compaction", compaction)
	}
}

// ------------------------------------------------
// CompactionExecutor
// ------------------------------------------------

type CompactionJob struct {
	destination uint32
	sstList     []sstable.Handle
	sortedRuns  []compaction2.SortedRun
}

type CompactionExecutor struct {
	options    *CompactorOptions
	tableStore *store.TableStore

	workerCh chan<- WorkerToOrchestratorMsg
	tasksWG  sync.WaitGroup
	abortCh  chan bool
	stopped  atomic.Bool
}

func newCompactorExecutor(
	options *CompactorOptions,
	workerCh chan<- WorkerToOrchestratorMsg,
	tableStore *store.TableStore,
) *CompactionExecutor {
	return &CompactionExecutor{
		options:    options,
		tableStore: tableStore,
		workerCh:   workerCh,
		abortCh:    make(chan bool),
	}
}

// create an iterator for CompactionJob.sstList and another iterator for CompactionJob.sortedRuns
// Return the merged iterator for the above 2 iterators
func (e *CompactionExecutor) loadIterators(compaction CompactionJob) (iter.KVIterator, error) {
	assert.True(
		!(len(compaction.sstList) == 0 && len(compaction.sortedRuns) == 0),
		"Compaction sources cannot be empty",
	)

	l0Iters := make([]iter.KVIterator, 0)
	for _, sst := range compaction.sstList {
		sstIter, err := sstable.NewIterator(&sst, e.tableStore.Clone())
		if err != nil {
			return nil, err
		}
		l0Iters = append(l0Iters, sstIter)
	}

	srIters := make([]iter.KVIterator, 0)
	for _, sr := range compaction.sortedRuns {
		srIter, err := compaction2.NewSortedRunIterator(sr, e.tableStore.Clone())
		if err != nil {
			return nil, err
		}
		srIters = append(srIters, srIter)
	}

	ctx := context.TODO()
	var l0MergeIter, srMergeIter iter.KVIterator
	if len(compaction.sortedRuns) == 0 {
		l0MergeIter = iter.NewMergeSort(ctx, l0Iters...)
		return l0MergeIter, nil
	} else if len(compaction.sstList) == 0 {
		srMergeIter = iter.NewMergeSort(ctx, srIters...)
		return srMergeIter, nil
	}

	it := iter.NewMergeSort(ctx, l0MergeIter, srMergeIter)
	return it, nil
}

func (e *CompactionExecutor) executeCompaction(compaction CompactionJob) (*compaction2.SortedRun, error) {
	allIter, err := e.loadIterators(compaction)
	if err != nil {
		return nil, err
	}
	var warn types.ErrWarn

	outputSSTs := make([]sstable.Handle, 0)
	currentWriter := e.tableStore.TableWriter(sstable.NewIDCompacted(ulid.Make()))
	currentSize := 0
	for {
		kv, ok := allIter.NextEntry(context.TODO())
		if !ok {
			if w := allIter.Warnings(); w != nil {
				warn.Merge(w)
			}
			break
		}

		value := kv.Value.GetValue()
		err = currentWriter.Add(kv.Key, value)
		if err != nil {
			return nil, err
		}

		currentSize += len(kv.Key)
		if value.IsPresent() {
			val, _ := value.Get()
			currentSize += len(val)
		}

		if uint64(currentSize) > e.options.MaxSSTSize {
			currentSize = 0
			finishedWriter := currentWriter
			currentWriter = e.tableStore.TableWriter(sstable.NewIDCompacted(ulid.Make()))
			sst, err := finishedWriter.Close()
			if err != nil {
				return nil, err
			}
			outputSSTs = append(outputSSTs, *sst)
		}
	}
	if currentSize > 0 {
		sst, err := currentWriter.Close()
		if err != nil {
			return nil, err
		}
		outputSSTs = append(outputSSTs, *sst)
	}
	return &compaction2.SortedRun{
		ID:      compaction.destination,
		SSTList: outputSSTs,
	}, warn.If()
}

func (e *CompactionExecutor) startCompaction(compaction CompactionJob) {
	if e.isStopped() {
		return
	}
	e.tasksWG.Add(1)
	go func() {
		defer e.tasksWG.Done()
		for {
			select {
			case <-e.abortCh:
				return
			default:
				sortedRun, err := e.executeCompaction(compaction)
				var msg WorkerToOrchestratorMsg
				if err != nil {
					// TODO(thrawn01): log the error somewhere.
					msg = WorkerToOrchestratorMsg{CompactionError: err}
				} else if sortedRun != nil {
					msg = WorkerToOrchestratorMsg{CompactionResult: sortedRun}
				}

				// wait till we can send msg to workerCh or abort is called
				for {
					select {
					case <-e.abortCh:
						return
					case e.workerCh <- msg:
					}
				}
			}
		}
	}()
}

func (e *CompactionExecutor) stop() {
	close(e.abortCh)
	e.tasksWG.Wait()
	e.stopped.Store(true)
}

func (e *CompactionExecutor) isStopped() bool {
	return e.stopped.Load()
}

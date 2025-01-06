package slatedb

import (
	"context"
	"errors"
	"github.com/oklog/ulid/v2"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/iter"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/common"
	compaction2 "github.com/slatedb/slatedb-go/slatedb/compaction"
	"github.com/slatedb/slatedb-go/slatedb/config"
	"github.com/slatedb/slatedb-go/slatedb/store"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type CompactionScheduler interface {
	maybeScheduleCompaction(state *CompactorState) []Compaction
}

type CompactorMainMsg int

const (
	CompactorShutdown CompactorMainMsg = iota + 1
)

type CompactionResult struct {
	SortedRun *compaction2.SortedRun
	Error     error
}

// Compactor - The CompactionOrchestrator checks with the CompactionScheduler if Level0 needs to be compacted.
// If compaction is needed, the CompactionOrchestrator gives CompactionJobs to the CompactionExecutor.
// The CompactionExecutor creates new goroutine for each CompactionJob and the results are written to a channel.
type Compactor struct {
	orchestrator *CompactionOrchestrator
}

func newCompactor(manifestStore *store.ManifestStore, tableStore *store.TableStore, opts config.DBOptions) (*Compactor, error) {
	orchestrator, err := spawnAndRunCompactionOrchestrator(manifestStore, tableStore, opts)
	if err != nil {
		return nil, err
	}

	return &Compactor{
		orchestrator: orchestrator,
	}, nil
}

func (c *Compactor) close() {
	c.orchestrator.shutdown()
}

// ------------------------------------------------
// CompactionOrchestrator
// ------------------------------------------------

func spawnAndRunCompactionOrchestrator(
	manifestStore *store.ManifestStore,
	tableStore *store.TableStore,
	opts config.DBOptions,
) (*CompactionOrchestrator, error) {
	orchestrator, err := newCompactionOrchestrator(opts, manifestStore, tableStore)
	if err != nil {
		return nil, err
	}

	orchestrator.spawnLoop(opts)
	return orchestrator, nil
}

type CompactionOrchestrator struct {
	options   *config.CompactorOptions
	manifest  *store.FenceableManifest
	state     *CompactorState
	scheduler CompactionScheduler
	executor  *CompactionExecutor

	// compactorMsgCh - When CompactionOrchestrator receives a CompactorShutdown message on this channel,
	// it calls executor.stop
	compactorMsgCh chan CompactorMainMsg
	waitGroup      sync.WaitGroup
	log            *slog.Logger
}

func newCompactionOrchestrator(
	opts config.DBOptions,
	manifestStore *store.ManifestStore,
	tableStore *store.TableStore,
) (*CompactionOrchestrator, error) {
	sm, err := store.LoadStoredManifest(manifestStore)
	if err != nil {
		return nil, err
	}
	if sm.IsAbsent() {
		return nil, common.ErrInvalidDBState
	}
	storedManifest, _ := sm.Get()

	manifest, err := store.NewCompactorFenceableManifest(&storedManifest)
	if err != nil {
		return nil, err
	}

	state, err := loadState(manifest)
	if err != nil {
		return nil, err
	}

	scheduler := loadCompactionScheduler()
	executor := newCompactorExecutor(opts.CompactorOptions, tableStore)

	o := CompactionOrchestrator{
		options:        opts.CompactorOptions,
		manifest:       manifest,
		state:          state,
		scheduler:      scheduler,
		executor:       executor,
		compactorMsgCh: make(chan CompactorMainMsg, 1),
		log:            opts.Log,
	}
	return &o, nil
}

func loadState(manifest *store.FenceableManifest) (*CompactorState, error) {
	dbState, err := manifest.DbState()
	if err != nil {
		return nil, err
	}
	return newCompactorState(dbState.Clone(), nil), nil
}

func loadCompactionScheduler() CompactionScheduler {
	return SizeTieredCompactionScheduler{}
}

func (o *CompactionOrchestrator) spawnLoop(opts config.DBOptions) {
	o.waitGroup.Add(1)
	go func() {
		defer o.waitGroup.Done()

		ticker := time.NewTicker(opts.CompactorOptions.PollInterval)
		defer ticker.Stop()

		for {
			resultPresent := o.processCompactionResult(opts.Log)
			if !resultPresent && o.executor.isStopped() {
				break
			}

			select {
			case <-ticker.C:
				err := o.loadManifest()
				assert.True(err == nil, "Failed to load manifest")
			case <-o.compactorMsgCh:
				// we receive Shutdown msg on compactorMsgCh. Stop the executor.
				// Don't return and let the loop continue until there are no more compaction results to process
				o.executor.stop()
				ticker.Stop()
			default:
			}
		}
	}()
}

func (o *CompactionOrchestrator) shutdown() {
	o.compactorMsgCh <- CompactorShutdown
	o.waitGroup.Wait()
}

func (o *CompactionOrchestrator) loadManifest() error {
	_, err := o.manifest.Refresh()
	if err != nil {
		return err
	}
	err = o.refreshDBState()
	if err != nil {
		return err
	}
	return nil
}

func (o *CompactionOrchestrator) refreshDBState() error {
	state, err := o.manifest.DbState()
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

func (o *CompactionOrchestrator) maybeScheduleCompactions() error {
	compactions := o.scheduler.maybeScheduleCompaction(o.state)
	for _, compaction := range compactions {
		err := o.submitCompaction(compaction)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *CompactionOrchestrator) startCompaction(compaction Compaction) {
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

func (o *CompactionOrchestrator) processCompactionResult(log *slog.Logger) bool {
	result, resultPresent := o.executor.nextCompactionResult()
	if resultPresent {
		if result.Error != nil {
			log.Error("Error executing compaction", "error", result.Error)
		} else if result.SortedRun != nil {
			err := o.finishCompaction(result.SortedRun)
			assert.True(err == nil, "Failed to finish compaction")
		}
	}
	return resultPresent
}

func (o *CompactionOrchestrator) finishCompaction(outputSR *compaction2.SortedRun) error {
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

func (o *CompactionOrchestrator) writeManifest() error {
	for {
		err := o.loadManifest()
		if err != nil {
			return err
		}

		core := o.state.dbState.Clone()
		err = o.manifest.UpdateDBState(core)
		if errors.Is(err, common.ErrManifestVersionExists) {
			o.log.Warn("conflicting manifest version. retry write", "error", err)
			continue
		}
		return err
	}
}

func (o *CompactionOrchestrator) submitCompaction(compaction Compaction) error {
	err := o.state.submitCompaction(compaction)
	if err != nil {
		o.log.Warn("invalid compaction", "error", err)
		return nil
	}
	o.startCompaction(compaction)
	return nil
}

func (o *CompactionOrchestrator) logCompactionState() {
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
	options    *config.CompactorOptions
	tableStore *store.TableStore

	resultCh chan CompactionResult
	tasksWG  sync.WaitGroup
	stopped  atomic.Bool
}

func newCompactorExecutor(
	options *config.CompactorOptions,
	tableStore *store.TableStore,
) *CompactionExecutor {
	return &CompactionExecutor{
		options:    options,
		tableStore: tableStore,
		resultCh:   make(chan CompactionResult, 1),
	}
}

func (e *CompactionExecutor) nextCompactionResult() (CompactionResult, bool) {
	select {
	case result := <-e.resultCh:
		return result, true
	default:
		return CompactionResult{}, false
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

		if e.isStopped() {
			return
		}

		var result CompactionResult
		sortedRun, err := e.executeCompaction(compaction)
		if err != nil {
			// TODO(thrawn01): log the error somewhere.
			result = CompactionResult{Error: err}
		} else if sortedRun != nil {
			result = CompactionResult{SortedRun: sortedRun}
		}
		e.resultCh <- result
	}()
}

func (e *CompactionExecutor) stop() {
	e.stopped.Store(true)
	e.waitForTasksToComplete()
}

func (e *CompactionExecutor) waitForTasksToComplete() {
	e.tasksWG.Wait()
}

func (e *CompactionExecutor) isStopped() bool {
	return e.stopped.Load()
}

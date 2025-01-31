package compaction

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/compacted"
	"github.com/slatedb/slatedb-go/slatedb/config"
	"github.com/slatedb/slatedb-go/slatedb/store"
)

type Orchestrator struct {
	options   *config.CompactorOptions
	manifest  *store.FenceableManifest
	State     *CompactorState
	scheduler Scheduler
	executor  *Executor

	// compactorMsgCh - When CompactionOrchestrator receives a CompactorShutdown message on this channel,
	// it calls executor.stop
	compactorMsgCh chan CompactorMainMsg
	waitGroup      sync.WaitGroup
	log            *slog.Logger
}

func NewOrchestrator(
	opts config.DBOptions,
	manifestStore *store.ManifestStore,
	tableStore *store.TableStore,
) (*Orchestrator, error) {
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
	executor := newExecutor(opts.CompactorOptions, tableStore)

	o := Orchestrator{
		options:        opts.CompactorOptions,
		manifest:       manifest,
		State:          state,
		scheduler:      scheduler,
		executor:       executor,
		compactorMsgCh: make(chan CompactorMainMsg, 1),
		log:            opts.Log,
	}
	return &o, nil
}

func (o *Orchestrator) spawnLoop(opts config.DBOptions) {
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

func (o *Orchestrator) shutdown() {
	o.compactorMsgCh <- CompactorShutdown
	o.waitGroup.Wait()
}

func (o *Orchestrator) loadManifest() error {
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

func (o *Orchestrator) refreshDBState() error {
	state, err := o.manifest.DbState()
	if err != nil {
		return err
	}

	o.State.RefreshDBState(state)
	err = o.maybeScheduleCompactions()
	if err != nil {
		return err
	}
	return nil
}

func (o *Orchestrator) maybeScheduleCompactions() error {
	compactions := o.scheduler.maybeScheduleCompaction(o.State)
	for _, compaction := range compactions {
		err := o.SubmitCompaction(compaction)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Orchestrator) startCompaction(compaction Compaction) {
	o.logCompactionState()
	dbState := o.State.DbState

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

	srsByID := make(map[uint32]compacted.SortedRun)
	for _, sr := range dbState.Compacted {
		srsByID[sr.ID] = sr
	}

	ssts := make([]sstable.Handle, 0)
	for _, sID := range compaction.sources {
		sstID, ok := sID.SstID().Get()
		if ok {
			ssts = append(ssts, sstsByID[sstID])
		}
	}

	sortedRuns := make([]compacted.SortedRun, 0)
	for _, sID := range compaction.sources {
		srID, ok := sID.SortedRunID().Get()
		if ok {
			sortedRuns = append(sortedRuns, srsByID[srID])
		}
	}

	o.executor.startCompaction(Job{
		destination: compaction.destination,
		sstList:     ssts,
		sortedRuns:  sortedRuns,
	})
}

func (o *Orchestrator) processCompactionResult(log *slog.Logger) bool {
	result, resultPresent := o.executor.nextCompactionResult()
	if resultPresent {
		if result.Error != nil {
			log.Error("Error executing compaction", "error", result.Error)
		} else if result.SortedRun != nil {
			err := o.FinishCompaction(result.SortedRun)
			assert.True(err == nil, "Failed to finish compaction")
		}
	}
	return resultPresent
}

func (o *Orchestrator) FinishCompaction(outputSR *compacted.SortedRun) error {
	o.State.FinishCompaction(outputSR)
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

func (o *Orchestrator) writeManifest() error {
	for {
		err := o.loadManifest()
		if err != nil {
			return err
		}

		core := o.State.DbState.Clone()
		err = o.manifest.UpdateDBState(core)
		if errors.Is(err, common.ErrManifestVersionExists) {
			o.log.Warn("conflicting manifest version. retry write", "error", err)
			continue
		}
		return err
	}
}

func (o *Orchestrator) SubmitCompaction(compaction Compaction) error {
	err := o.State.SubmitCompaction(compaction)
	if err != nil {
		o.log.Warn("invalid compaction", "error", err)
		return nil
	}
	o.startCompaction(compaction)
	return nil
}

func (o *Orchestrator) logCompactionState() {
	// LogState(o.log, o.state.dbState)
	for _, compaction := range o.State.Compactions {
		o.log.Info("in-flight compaction", "compaction", compaction)
	}
}

func (o *Orchestrator) WaitForTasksCompletion() {
	o.executor.waitForTasksCompletion()
}

func (o *Orchestrator) NextCompactionResult() (Result, bool) {
	return o.executor.nextCompactionResult()
}

func loadState(manifest *store.FenceableManifest) (*CompactorState, error) {
	dbState, err := manifest.DbState()
	if err != nil {
		return nil, err
	}
	return NewCompactorState(dbState.Clone(), nil), nil
}

func loadCompactionScheduler() Scheduler {
	return SizeTieredCompactionScheduler{}
}

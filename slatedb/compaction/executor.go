package compaction

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/oklog/ulid/v2"
	"github.com/slatedb/slatedb-go/internal/assert"
	"github.com/slatedb/slatedb-go/internal/iter"
	"github.com/slatedb/slatedb-go/internal/sstable"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/slatedb/slatedb-go/slatedb/compacted"
	"github.com/slatedb/slatedb-go/slatedb/config"
	"github.com/slatedb/slatedb-go/slatedb/store"
)

type Executor struct {
	options    *config.CompactorOptions
	tableStore *store.TableStore

	resultCh chan Result
	tasksWG  sync.WaitGroup
	stopped  atomic.Bool
}

func newExecutor(
	options *config.CompactorOptions,
	tableStore *store.TableStore,
) *Executor {
	return &Executor{
		options:    options,
		tableStore: tableStore,
		resultCh:   make(chan Result, 1),
	}
}

func (e *Executor) nextCompactionResult() (Result, bool) {
	select {
	case result := <-e.resultCh:
		return result, true
	default:
		return Result{}, false
	}
}

// create an iterator for CompactionJob.sstList and another iterator for CompactionJob.sortedRuns
// Return the merged iterator for the above 2 iterators
func (e *Executor) loadIterators(compaction Job) (iter.KVIterator, error) {
	assert.True(
		!(len(compaction.sstList) == 0 && len(compaction.sortedRuns) == 0),
		"Compaction sources cannot be empty",
	)

	l0Iters := make([]iter.KVIterator, 0)
	for _, sst := range compaction.sstList {
		ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
		sstIter, err := sstable.NewIterator(ctx, &sst, e.tableStore.Clone())
		cancel()
		if err != nil {
			return nil, err
		}
		l0Iters = append(l0Iters, sstIter)
	}

	srIters := make([]iter.KVIterator, 0)
	for _, sr := range compaction.sortedRuns {
		ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
		srIter, err := compacted.NewSortedRunIterator(ctx, sr, e.tableStore.Clone())
		cancel()
		if err != nil {
			return nil, err
		}
		srIters = append(srIters, srIter)
	}

	var l0MergeIter, srMergeIter iter.KVIterator
	ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
	defer cancel()
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

func (e *Executor) executeCompaction(compaction Job) (*compacted.SortedRun, error) {
	allIter, err := e.loadIterators(compaction)
	if err != nil {
		return nil, err
	}
	var warn types.ErrWarn

	outputSSTs := make([]sstable.Handle, 0)
	currentWriter := e.tableStore.TableWriter(sstable.NewIDCompacted(ulid.Make()))
	currentSize := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
		kv, ok := allIter.NextEntry(ctx)
		cancel()
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
			ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
			sst, err := finishedWriter.Close(ctx)
			cancel()
			if err != nil {
				return nil, err
			}
			outputSSTs = append(outputSSTs, *sst)
		}
	}
	if currentSize > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
		sst, err := currentWriter.Close(ctx)
		cancel()
		if err != nil {
			return nil, err
		}
		outputSSTs = append(outputSSTs, *sst)
	}
	return &compacted.SortedRun{
		ID:      compaction.destination,
		SSTList: outputSSTs,
	}, warn.If()
}

func (e *Executor) startCompaction(compaction Job) {
	if e.isStopped() {
		return
	}

	e.tasksWG.Add(1)
	go func() {
		defer e.tasksWG.Done()

		if e.isStopped() {
			return
		}

		var result Result
		sortedRun, err := e.executeCompaction(compaction)
		if err != nil {
			// TODO(thrawn01): log the error somewhere.
			result = Result{Error: err}
		} else if sortedRun != nil {
			result = Result{SortedRun: sortedRun}
		}
		e.resultCh <- result
	}()
}

func (e *Executor) stop() {
	e.stopped.Store(true)
	e.waitForTasksCompletion()
}

func (e *Executor) waitForTasksCompletion() {
	e.tasksWG.Wait()
}

func (e *Executor) isStopped() bool {
	return e.stopped.Load()
}

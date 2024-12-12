# 4. compaction

Date: 2024-12-11

## Status

Accepted

## Context

Compaction is a crucial process in LSM-tree based databases to manage and optimize data storage. It's necessary to
understand how SlateDB implements compaction to maintain performance and storage efficiency.

## Decision

SlateDB implements compaction with the following key components and processes:

### Compaction Structure
- Level0 consists of a list of SSTs (Sorted String Tables).
- The job of Compactor is to merge SSTs from Level0 into lower levels (L1, L2, and so on).
- Lower levels are referred to as sorted runs in SlateDB. Each SST in a sorted run contains a distinct subset of the keyspace.

### Compaction Components
1. Orchestrator
2. Scheduler
3. Executor

### Compaction Process
1. The Orchestrator checks with the Scheduler if Level0 needs to be compacted.
2. If compaction is needed, the Orchestrator gives CompactionJobs to the Executor.
3. The Executor executes compactions and returns results to the Orchestrator.

### Compaction Workflow
When DB is opened, a background goroutine for Orchestrator is started which does the following:

1. Every PollInterval duration:
    - Read manifest from ObjectStore
    - Check with Scheduler for any compactions needed
    - For each compaction that scheduler returns:
      - Store compaction details in a CompactionState
      - Create a CompactionJob and pass it to Executor
2. The Executor starts a new Worker goroutine to run each CompactionJob.
3. The result of running the CompactionJob is communicated back to Orchestrator through a channel.
4. When a Worker goroutine finishes a CompactionJob:
    - Update DBState by removing L0 SSTs that have been compacted and adding new compacted SortedRuns
    - Write the updated Manifest to ObjectStore
5. On receiving a CompactorShutdown message (which happens when DB.Close() is called):
    - Shut down all Executor goroutines
    - Wait until all Executor goroutines shutdown and return

## Consequences

- Improved read performance over time as data is organized into sorted runs.
- Reduced storage space usage by merging and removing obsolete data.
- Background compaction process allows for continuous system operation without blocking writes.
- Complexity in managing multiple levels and ensuring consistency during compaction.
- Potential for increased write amplification due to repeated data rewrites during compaction.
- Need for careful tuning of compaction parameters to balance between read performance, write performance, and storage efficiency.

# 3. read and write sequence

Date: 2024-12-11

## Status

Accepted

## Context

Describe at a high level the read and write path data takes through the slateDB system

## Decision

### Opening the Database
When the DB is opened, we start 3 background goroutines:
1. WALFlushTask (write WAL contents to ObjectStore, then write WAL contents to Memtable)
2. MemtableFlushTask (write memtable contents to ObjectStore (L0 level))
3. Compactor (compact L0 level to further levels/SortedRuns)

### Sequence of ops for PUT
(Assumption AwaitFlush is true i.e. PUT will wait till write is durably committed)

1. Write KV to WAL
2. Every FlushInterval duration, a background goroutine WALFLushTask will do the following:
    - Freeze WAL to ImmutableWAL
    - Flush each ImmutableWAL present to ObjectStore and then to Memtable
    - If Memtable has reached size L0SSTSizeBytes then freeze Memtable to ImmutableMemtable and notify MemtableFlushTask goroutine through memtableFlushNotifierCh Channel
3. Return

Another background goroutine (MemtableFlushTask) will do the following:
1. If a notification is received on memtableFlushNotifierCh Channel from WALFlushTask goroutine then:
    - Flush each ImmutableMemtable to Level0 on ObjectStore
    - Update DBState
    - Write the DBState (Manifest) to ObjectStore
2. Every ManifestPollInterval duration we read Manifest from ObjectStore and update the DBState in memory

### Sequence of ops for GET
(Assumption ReadLevel is Committed i.e. GET will read data that is durably committed)

1. Search Memtable, if we find key then return value
2. Search ImmutableMemtable, if we find key then return value
3. Search SSTs in Level0, if we find key then return value
4. Search compacted levels, if we find key then return value

If we don't find key, return error ErrKeyNotFound

### Manifest
Manifest provides a way to keep each task updated on the latest DBState. Both MemtableFlushTask and Compactor read the
latest manifest from ObjectStore at regular intervals. They write the manifest to ObjectStore if they modify any
DBState (Level0 SSTs or Compacted Sorted runs).

## Consequences

- The system ensures data durability by writing to WAL before acknowledging writes.
- Read operations check multiple levels, potentially impacting read latency for keys in lower levels.
- The use of background tasks for flushing and compaction allows for better write throughput but introduces some complexity in managing the system state.
- The manifest provides a consistent view of the database state across different components, but requires regular I/O operations to stay updated.

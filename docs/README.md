
## Introduction

This document aims to give some implementation details.   

Go implementation is behind Rust implementation so things written here might get outdated as we progress
(we will try to update this whenever we can).

Before reading this, it would be helpful to read the documentation on the website [slatedb](https://slatedb.io/docs/architecture)


## Open DB

When the DB is opened, we start 3 background goroutines
1. [WALFlushTask](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L67) (write WAL contents to ObjectStore, then write WAL contents to Memtable)
2. [MemtableFlushTask](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L71) (write memtable contents to ObjectStore (L0 level) )
3. [Compactor](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L75) (compact L0 level to further levels/SortedRuns)



## Sequence of ops for PUT

(Assumption [AwaitFlush](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/config.go#L122) is true i.e. PUT will wait till write is durably committed)

1. write [KV to WAL](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L112)
2. every [FlushInterval](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L19) duration, a background goroutine WALFLushTask will do the following
    - Freeze [WAL to ImmutableWAL](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L41)
    - Flush each ImmutableWAL present to [ObjectStore](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L56) and then to [Memtable](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L63)
    - If Memtable has reached size [L0SSTSizeBytes](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/config.go#L71) then freeze [Memtable to ImmutableMemtable](https://github.com/slatedb/slatedb-go/blob/main/slatedb/flush.go#L64) and [notify](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L296) MemtableFlushTask goroutine through memtableFlushNotifierCh Channel
3. return

another background goroutine(MemtableFlushTask) will do the following:
1. If a [notification](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L152C18-L152C41) is received on memtableFlushNotifierCh Channel from WALFlushTask goroutine then we 
    - [flush each ImmutableMemtable](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L224) to Level0 on ObjectStore
    - [update DBState](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L229)
    - write the [DBState(Manifest) to ObjectStore](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L230)
2. Every [ManifestPollInterval](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L140) duration we read Manifest from ObjectStore and [update](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L148) the DBState in memory   
   (Note: [Manifest/DBState](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db_state.go#L35) contain info about L0 levels, Compacted levels)



## Sequence of ops for GET

(Assumption ReadLevel is [Committed](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/config.go#L97) i.e. GET will read data that is durably committed)

1. [search Memtable](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L152), if we find key then return value
2. [search ImmutableMemtable](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L157), if we find key then return value
3. [search SSTs in Level0](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L167), if we find key then return value
4. [search compacted levels](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L182), if we find key then return value

If we don't find key, return error ErrKeyNotFound



## Compaction

Level0 consists of a list of SSTs(Sorted String Table). (Note: The SSTs internally are a collection of Blocks with each Block having a list of KV pairs)  
The job of Compactor is to merge SSTs from Level0 into lower levels (L1, L2, and so on). These lower levels are referred to as sorted runs in SlateDB. Each SST in a sorted run contains a distinct subset of the keyspace.

There are 3 components - Orchestrator, Scheduler, Executor.  

The Orchestrator first checks with the Scheduler if Level0 needs to be compacted.  
If the Scheduler says that Compaction is needed, then Orchestrator gives the CompactionJobs to Executor which executes compactions and returns result to Orchestrator.

When DB is opened, a background goroutine for Orchestrator is [started](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L49) which does the following:  
1. Every [PollInterval](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L89) duration,
    - [read manifest from ObjectStore](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L187)
    - [Check with Scheduler](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L205)  for any compactions needed,
    - [For each compaction](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L214) that [scheduler](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/size_tiered_compaction.go#L7) returns
      - [store compaction details](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L302) in a CompactionState
      - [Create a CompactionJob](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L262) and pass it to Executor. Executor [starts a new Worker goroutine](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L448) to run the CompactionJob. The result of runnning the CompactionJob is [communicated back to Orchestrator](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L468) through workerCh channel
2. If any Worker goroutine has finished running a CompactionJob and returned the result through [workerCh channel](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L97)
    - [Update DBState](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L270) by removing L0 SSTs that have been compacted and adding new compacted SortedRuns
    - Write the updated [Manifest to ObjectStore](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L272)
3. If we [receive CompactorShutdown](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L104) message(which happens when DB.Close() is called)
    - Shut down all Executor goroutines by sending abort message through [abortCh](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L478)
    - Wait till all Executor goroutines shutdown and return

    
## Close DB

When DB is closed we send a message through channels to the 3 goroutines to Shutdown and wait till they shutdown.  

1. Send [CompactorShutdown message to Compactor](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/compactor.go#L61)
2. [Send a message](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L94)  to WALFlushTask to shutdown
3. [Send a message](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L98)  to MemtableFlushTask to shutdown


## Manifest

We have mentioned a few times above that we write manifest to ObjectStore and read manifest from ObjectStore  

Why is Manifest needed ?  

Consider the 2 tasks below which run in different goroutines.
1. MemtableFlushTask keeps adding new Level0 SSTs
2. Compactor keeps flushing Level0 SSTs to further levels or SortedRuns

The two tasks above need to know the latest state of Level0 SSTs to perform their operation. Manifest provides a way to keep each other updated on the latest DBState.  
So both the tasks read the latest manifest from ObjectStore at regular intervals.  
And they write the manifest to ObjectStore if they modify any DBState(Level0 SSTs or Compacted Sorted runs)  





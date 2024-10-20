
## Introduction

This document aims to give some implementation details (might be useful if anyone wants to contribute).   
Go implementation is behind Rust implementation so things written here might get outdated as we progress
(we will try to update this whenever we can).

https://github.com/slatedb/slatedb-go/tree/ca863aba66169f88186f3aa970bdf02df891e0de

Before reading this, it would be helpful to read the documentation on the website [slatedb](https://slatedb.io/docs/architecture)

## Open DB

When the DB is opened, we start 3 background threads
1. [WALFlushTask](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L67) (write WAL contents to ObjectStore, write WAL contents to Memtable)
2. [MemtableFlushTask](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L71) (write memtable contents to ObjectStore (L0 level) )
3. [Compactor](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L75) (compact L0 level to higher levels)



## Sequence of ops for PUT

(Assumption [AwaitFlush](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/config.go#L122) is true i.e. PUT will wait till write is durably committed)

1. write [KV to WAL](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L112)
2. every [FlushInterval](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L19) duration, a background thread WALFLushTask will run which does the following
    - Freeze [WAL to ImmutableWAL](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L41)
    - Flush each ImmutableWAL present to [ObjectStore](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L56) and then to [Memtable](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L63)
    - If Memtable has reached size [L0SSTSizeBytes](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/config.go#L71) then freeze [Memtable to ImmutableMemtable](https://github.com/slatedb/slatedb-go/blob/main/slatedb/flush.go#L64) and [notify](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db.go#L296) MemtableFlushTask thread through memtableFlushNotifierCh Channel
3. return

another background thread(MemtableFlushTask) will be doing the following:
1. Every [ManifestPoll](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L140) duration read Manifest from ObjectStore and [update](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L148) the DBState in memory   
   (Note: [Manifest/DBState](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/db_state.go#L35) contain info about L0 levels, Compacted levels)
2. If a [notification](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L152C18-L152C41) is received on memtableFlushNotifierCh Channel from WALFlushTask thread then we [flush each ImmutableMemtable](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L224) to Level0 on ObjectStore and [update DBState](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/flush.go#L229)

another background thread(compactorThread) will compact Level0 data (list of SSTs) to higher levels

## sequence of ops for GET

(Assumption ReadLevel is [Committed](https://github.com/slatedb/slatedb-go/blob/ca863aba66169f88186f3aa970bdf02df891e0de/slatedb/config.go#L97) i.e. GET will read data that is durably committed)

1. search in-memory Memtable, if we find key return
2. search SSTs in Level0, if we find key return
3. search higher levels, if we find key return


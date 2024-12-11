# 5. sstable codec

Date: 2024-12-11

## Status

Accepted

## Context

The Sorted String Table (SSTable) is a fundamental data structure in LSM-tree based databases like SlateDB. It's 
crucial to define a clear and efficient format for SSTable to ensure optimal storage and retrieval of data. This ADR
outlines the structure and format of SSTables in SlateDB.

## Decision

At a high level, the SSTable consists of the following components in this order:
1. List of `Blocks` (where each Block contains KeyValue pairs)
2. `BloomFilter` if the number of keys in SSTable is at least DBOptions.MinFilterKeys
3. `SsTableIndex` which contains the `Offset` and `FirstKey` of each Block added above
4. `SsTableInfo` which contains meta information of the SSTable like offset, length of `BloomFilter` and offset, length of `SsTableIndex`
5. Finally, the offset (4 bytes) of `SsTableInfo`

Note: The WAL `.sst` files stored under the `wal/` directory of object storage and the compacted `.sst` files stored
under the `compacted/` directory of object storage have the same SSTable format. The compacted directory includes
`.sst` files for both Level0 and SortedRun.

### KeyValue Format

```
If not a tombstone then KeyValue is represented as
 +-----------------------------------------------+
 |               KeyValue                        |
 +-----------------------------------------------+
 |  +-----------------------------------------+  |
 |  |  Key Length (2 bytes)                   |  |
 |  +-----------------------------------------+  |
 |  |  Key                                    |  |
 |  +-----------------------------------------+  |
 |  |  Value Length (4 bytes)                 |  |
 |  +-----------------------------------------+  |
 |  |  Value                                  |  |
 |  +-----------------------------------------+  |
 +-----------------------------------------------+

 If it is a tombstone then KeyValue is represented as.

 +-----------------------------------------------+
 |               KeyValue (Tombstone)            |
 +-----------------------------------------------+
 |  +-----------------------------------------+  |
 |  |  Key Length (2 bytes)                   |  |
 |  +-----------------------------------------+  |
 |  |  Key                                    |  |
 |  +-----------------------------------------+  |
 |  |  Tombstone (4 bytes)                    |  |
 |  +-----------------------------------------+  |
 +-----------------------------------------------+
```

### Block Format

Each Block contains the following (Assume Block contains 'n' KeyValue pairs):
```
 +-----------------------------------------------+
 |               Block                           |
 +-----------------------------------------------+
 |  +-----------------------------------------+  |
 |  |  Block.Data                             |  |
 |  |  (List of KeyValues)                    |  |
 |  |  +-----------------------------------+  |  |
 |  |  | KeyValue Pair                     |  |  |
 |  |  +-----------------------------------+  |  |
 |  |  ... repeat 'n' KeyValue Pairs       |  |  |
 |  +-----------------------------------------+  |
 |  +-----------------------------------------+  |
 |  |  Block.Offsets                          |  |
 |  |  +-----------------------------------+  |  |
 |  |  |  Offset of KeyValue (2 bytes)     |  |  |
 |  |  +-----------------------------------+  |  |
 |  |  ... repeat 'n' offsets                 |  |
 |  +-----------------------------------------+  |
 |  +-----------------------------------------+  |
 |  |  Number of Offsets (2 bytes)            |  |
 |  +-----------------------------------------+  |
 |  |  Checksum (4 bytes)                     |  |
 |  +-----------------------------------------+  |
 +-----------------------------------------------+
```

### BloomFilter Format

Assume number of keys added to BloomFilter is 'n'
```
╭─────────────────────┬────────────────────╮
│numberOfHashFunctions│ bitArray of filter │
├─────────────────────┼────────────────────┤
│2 bytes              │ n * bitsPerKey bits│
╰─────────────────────┴────────────────────╯
```

### SsTableIndex Format

SsTableIndex contains the `Offset` and `FirstKey` of each Block present in the SSTable. This is serialized to bytes using flatbuffers.

```go
type SsTableIndex struct {
	BlockMeta []*BlockMeta
}

type BlockMeta struct {
	Offset   uint64
	FirstKey []byte
}
```

### SsTableInfo Format

SsTableInfo contains the meta information of the SSTable. This is serialized to bytes using flatbuffers.

```go
type SsTableInfo struct {
    // contains the firstKey of the SSTable
    FirstKey          []byte
    
    // the offset at which SsTableIndex starts when SSTable is serialized.
    // SsTableIndex holds the meta info about each block.
    IndexOffset       uint64
    
    // the length of the SSTableIndex.
    IndexLen          uint64
    
    // the offset at which BloomFilter starts when SSTable is serialized.
    FilterOffset      uint64
    
    // the length of the BloomFilter
    FilterLen         uint64
    
    // the codec used to compress/decompress SSTable before serializing/desirializing
    CompressionFormat CompressionFormat
}
```

The entire SSTable format 
```
 +-----------------------------------------------+
 |               SSTable                         |
 +-----------------------------------------------+
 |  +-----------------------------------------+  |
 |  |  List of Blocks                         |  |
 |  |  +-----------------------------------+  |  |
 |  |  |  block.Block                      |  |  |
 |  |  |  +-------------------------------+|  |  |
 |  |  |  |  List of KeyValue pairs        |  |  |
 |  |  |  |  +---------------------------+ |  |  |
 |  |  |  |  |  Key Length (2 bytes)     | |  |  |
 |  |  |  |  |  Key                      | |  |  |
 |  |  |  |  |  Value Length (4 bytes)   | |  |  |
 |  |  |  |  |  Value                    | |  |  |
 |  |  |  |  +---------------------------+ |  |  |
 |  |  |  |  ...                           |  |  |
 |  |  |  +-------------------------------+|  |  |
 |  |  |  |  Offsets for each Key          |  |  |
 |  |  |  |  (n * 2 bytes)                 |  |  |
 |  |  |  +-------------------------------+|  |  |
 |  |  |  |  Number of Offsets (2 bytes)   |  |  |
 |  |  |  +-------------------------------+|  |  |
 |  |  |  |  Checksum (4 bytes)            |  |  |
 |  |  +-----------------------------------+  |  |
 |  |  ...                                    |  |
 |  +-----------------------------------------+  |
 |                                               |
 |  +-----------------------------------------+  |
 |  |  bloom.Filter (if MinFilterKeys met)    |  |
 |  +-----------------------------------------+  |
 |                                               |
 |  +-----------------------------------------+  |
 |  |  flatbuf.SsTableIndexT                  |  |
 |  |  (List of Block Offsets)                |  |
 |  |  - Block Offset (Start of Block)        |  |
 |  |  - FirstKey of this Block               |  |
 |  |  ...                                    |  |
 |  +-----------------------------------------+  |
 |                                               |
 |  +-----------------------------------------+  |
 |  |  flatbuf.SsTableInfoT                   |  |
 |  |  - FirstKey of the SSTable              |  |
 |  |  - Offset of bloom.Filter               |  |
 |  |  - Length of bloom.Filter               |  |
 |  |  - Offset of flatbuf.SsTableIndexT      |  |
 |  |  - Length of flatbuf.SsTableIndexT      |  |
 |  |  - The Compression Codec                |  |
 |  +-----------------------------------------+  |
 |  |  Checksum of SsTableInfoT (4 bytes)     |  |
 |  +-----------------------------------------+  |
 |                                               |
 |  +-----------------------------------------+  |
 |  |  Offset of SsTableInfoT (4 bytes)       |  |
 |  +-----------------------------------------+  |
 +-----------------------------------------------+
```

## Consequences

- Efficient storage and retrieval: The structured format allows for quick access to specific parts of the SSTable, improving read performance.
- Flexibility: The format supports both regular key-value pairs and tombstones, allowing for efficient deletion operations.
- Versioning considerations: Any future changes to this format will require careful handling to maintain backwards compatibility.

Note: Currently, we are using compression for Block, BloomFilter, and SsTableIndex on the serialized data before
writing to object storage if the user has initialized DB with DBOptions.CompressionCodec.

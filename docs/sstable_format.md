
## SSTable

At a high level the SSTable consists of the following in this order:
1. List of `Blocks` (where each Block contains KeyValue pairs)
2. `BloomFilter` if the number of keys in SSTable is atleast DBOptions.MinFilterKeys
3. `SsTableIndex` which contains the `Offset` and `FirstKey` of each Block added above
4. `SsTableInfo` which contains meta information of the SSTable like offset, length of `BloomFilter` and offset, length of `SsTableIndex`
5. Finally the offset(4 bytes) of `SsTableInfo`

Note: The WAL `.sst` files stored under `wal/` directory of object storage and  
the compacted `.sst` files stored under `compacted/` directory of object storage have the same SSTable format.    
The compacted directory includes `.sst` files for both Level0 and SortedRun.


### KeyValue format
```
If not a tombstone then KeyValue is represented as
╭─────────┬────────────────┬────────────┬──────────────────╮
│keyLength│ key            │ valueLength│ value            │
├─────────┼────────────────┼────────────┼──────────────────┤
│2 bytes  │ keyLength bytes│ 4 bytes    │ valueLength bytes│
╰─────────┴────────────────┴────────────┴──────────────────╯

If it is a tombstone then KeyValue is represented as
╭─────────┬────────────────┬──────────╮
│keyLength│ key            │ Tombstone│
├─────────┼────────────────┼──────────┤
│2 bytes  │ keyLength bytes│ 4 bytes  │
╰─────────┴────────────────┴──────────╯
```

### Block format
Each Block contains the following: (Assume Block contains 'n' KeyValue pairs)
```
╭────────╮
│KeyValue│ ... repeat 'n' KeyValue pairs in above KeyValue format.
╰────────╯

Then we have offsets of each KeyValue pair
╭───────╮
│offset │
├───────┤ ... repeat 'n' offsets
│2 bytes│
╰───────╯

Then we have KeyValue pair count 'n'
╭────────────────╮
│KeyValue count n│
├────────────────┤
│2 bytes         │
╰────────────────╯

Then we have checksum of the above data combined
╭────────╮
│checksum│
├────────┤
│4 bytes │
╰────────╯
```

### BloomFilter format 
Assume number of keys added to BloomFilter is 'n'
```
╭─────────────────────┬────────────────────╮
│numberOfHashFunctions│ bitArray of filter │
├─────────────────────┼────────────────────┤
│2 bytes              │ n * bitsPerKey bits│
╰─────────────────────┴────────────────────╯
```

### SsTableIndex format
SsTableIndex contains the `Offset` and `FirstKey` of each Block present in the SSTable. 
This is serialized to bytes using flatbuffers

```
type SsTableIndex struct {
	BlockMeta []*BlockMeta
}

type BlockMeta struct {
	Offset   uint64
	FirstKey []byte
}
```


### SsTableInfo format
SsTableInfo contains the meta information of the SSTable
This is serialized to bytes using flatbuffers

The format is as follows
```
╭──────────────────────┬─────────╮
│serialized SsTableInfo│ checksum│
├──────────────────────┼─────────┤
│                      │ 4 bytes │
╰──────────────────────┴─────────╯
```

```
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


Note: Currently we are using compression for Block, BloomFIlter and SsTableIndex on the serialized data before writing to object storage if the user has initialized DB with DBOptions.CompressionCodec 
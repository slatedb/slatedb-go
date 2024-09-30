## Introduction

slatedb-go is a Go port of [slatedb](https://github.com/slatedb/slatedb)

SlateDB is an embedded storage engine built as a [log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree). Unlike traditional LSM-tree storage engines, SlateDB writes data to object storage (S3, GCS, ABS, MinIO, Tigris, and so on). Leveraging object storage allows SlateDB to provide bottomless storage capacity, high durability, and easy replication. The trade-off is that object storage has a higher latency and higher API cost than local disk.

To mitigate high write API costs (PUTs), SlateDB batches writes. Rather than writing every `put()` call to object storage, MemTables are flushed periodically to object storage as a string-sorted table (SST). The flush interval is configurable.

Checkout [slatedb.io](https://slatedb.io) to learn more.

## Get Started

```Go
package main

import (
	"fmt"
	"github.com/naveen246/slatedb-go/slatedb"
	"github.com/thanos-io/objstore"
)

func main() {
	bucket := objstore.NewInMemBucket()
	db, _ := slatedb.Open("/tmp/testDB", bucket)

	key := []byte("key1")
	value := []byte("value1")

	db.Put(key, value)
	fmt.Println("Put:", string(key), string(value))

	data, _ := db.Get(key)
	fmt.Println("Get:", string(key), string(data))

	db.Delete(key)
	data, err := db.Get(key)
	if err != nil && err.Error() == "key not found" {
		fmt.Println("Delete:", string(key))
	} else {
		fmt.Println("failed to delete key", string(key))
	}

	db.Close()
}
```

## Features

SlateDB is currently in the early stages of development. It is not yet ready for production use.

## License

SlateDB is licensed under the Apache License, Version 2.0.
<a href="https://slatedb.io">
  <img src="https://github.com/slatedb/slatedb-website/blob/main/assets/png/gh-banner.png?raw=true" alt="SlateDB" width="100%">
</a>

![GitHub License](https://img.shields.io/github/license/slatedb/slatedb?style=flat-square)
<a href="https://slatedb.io">![slatedb.io](https://img.shields.io/badge/site-slatedb.io-00A1FF?style=flat-square)</a>
<a href="https://discord.gg/mHYmGy5MgA">![Discord](https://img.shields.io/discord/1232385660460204122?style=flat-square)</a>

## Introduction

slatedb-go is a Go port of [slatedb](https://github.com/slatedb/slatedb)

SlateDB is an embedded storage engine built as a [log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree). Unlike traditional LSM-tree storage engines, SlateDB writes data to object storage (S3, GCS, ABS, MinIO, Tigris, and so on). Leveraging object storage allows SlateDB to provide bottomless storage capacity, high durability, and easy replication. The trade-off is that object storage has a higher latency and higher API cost than local disk.

To mitigate high write API costs (PUTs), SlateDB batches writes. Rather than writing every `put()` call to object storage, MemTables are flushed periodically to object storage as a string-sorted table (SST). The flush interval is configurable.

Checkout [slatedb.io](https://slatedb.io) to learn more.

## Get Started

```Go
package main

import (
   "context"
   "fmt"
   "log/slog"
   "time"

   "github.com/slatedb/slatedb-go/slatedb"
   "github.com/thanos-io/objstore"
)

func main() {
   bucket := objstore.NewInMemBucket()
   ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
   defer cancel()
   db, _ := slatedb.Open(ctx, "/tmp/testDB", bucket)

   key := []byte("key1")
   value := []byte("value1")

   db.Put(key, value)
   fmt.Println("Put:", string(key), string(value))

   data, _ := db.Get(ctx, key)
   fmt.Println("Get:", string(key), string(data))

   db.Delete(key)
   _, err := db.Get(ctx, key)
   if err != nil && err.Error() == "key not found" {
      fmt.Println("Delete:", string(key))
   } else {
      slog.Error("Unable to delete", "error", err)
   }

   if err := db.Close(); err != nil {
      slog.Error("Error closing db", "error", err)
   }
}
```

## Features

SlateDB is currently in the early stages of development. It is not yet ready for production use.

## License

SlateDB is licensed under the Apache License, Version 2.0.

## FAQ

1. Why is there a Go port instead of using Go binding ?

   We wanted developers using this library in Go to be able to easily understand and modify(if needed) the internals without having to learn a new language.

   Go developers will also have an option to use Go binding(when it is ready) if they can use cgo/ffi.


2. Is there a risk of a drift between the inner workings of the Rust and Go implementation?

   We will try to keep it close to the Rust implementation.
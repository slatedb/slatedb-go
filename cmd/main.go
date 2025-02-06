package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/thanos-io/objstore"

	"github.com/slatedb/slatedb-go/slatedb"
)

func main() {

	bucket := objstore.NewInMemBucket()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, _ := slatedb.Open(ctx, "/tmp/testDB", bucket)

	key := []byte("key1")
	value := []byte("value1")

	_ = db.Put(ctx, key, value)
	fmt.Println("Put:", string(key), string(value))

	data, _ := db.Get(ctx, key)
	fmt.Println("Get:", string(key), string(data))

	_ = db.Delete(ctx, key)
	_, err := db.Get(ctx, key)
	if err != nil && err.Error() == "key not found" {
		fmt.Println("Delete:", string(key))
	} else {
		slog.Error("Unable to delete", "error", err)
	}

	if err := db.Close(ctx); err != nil {
		slog.Error("Error closing db", "error", err)
	}
}

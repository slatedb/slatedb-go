package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/thanos-io/objstore"

	"github.com/slatedb/slatedb-go/slatedb"
	"github.com/slatedb/slatedb-go/slatedb/common"
)

func main() {
	bucket := objstore.NewInMemBucket()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, _ := slatedb.Open(ctx, "/tmp/testDB", bucket)

	key := []byte("key1")
	value := []byte("value1")

	db.Put(key, value)
	slog.With("key", string(key)).With("value", string(value)).Info("Put into slatedb")

	data, _ := db.Get(ctx, key)
	slog.With("key", string(key)).With("value", string(data)).Info("Get from slatedb")

	db.Delete(key)
	_, err := db.Get(ctx, key)
	if errors.Is(err, common.ErrKeyNotFound) {
		slog.With("key", string(key)).Info("Key deleted")
	} else {
		slog.With("err", err).Error("Unable to delete")
	}

	if err := db.Close(); err != nil {
		slog.With("err", err).Error("Error closing db")
	}
}

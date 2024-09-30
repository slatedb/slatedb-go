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

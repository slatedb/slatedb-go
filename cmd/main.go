package main

import (
	"fmt"
	"github.com/naveen246/slatedb-go/slatedb"
	"github.com/thanos-io/objstore"
)

func main() {
	bucket := objstore.NewInMemBucket()
	db, err := slatedb.Open("/tmp/testDB", bucket)
	if err != nil {
		fmt.Println(err)
		return
	}

	key := []byte("key")
	value := []byte("value")

	db.Put(key, value)
	fmt.Println("Put", string(key), string(value))

	data, err := db.Get(key)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Get value is ", string(data))

	db.Delete(key)
	fmt.Println("Key deleted is ", string(data))

	db.Close()

}

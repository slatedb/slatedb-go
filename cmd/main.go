package main

import (
	"fmt"
	"github.com/naveen246/slatedb-go/slatedb"
	"github.com/thanos-io/objstore"
)

func main() {
	bucket := objstore.NewInMemBucket()
	options := slatedb.DefaultOptions()
	db, err := slatedb.Open(options, "/tmp/testDB", bucket)
	if err != nil {
		fmt.Println(err)
		return
	}

	key := []byte("key")
	value := []byte("value")

	err = db.Put(key, value)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Put", string(key), string(value))

	data, err := db.Get(key)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Get value is ", string(data))

	err = db.Delete(key)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Key deleted is ", string(data))

	db.Close()

}

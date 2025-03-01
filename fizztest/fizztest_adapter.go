package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/oklog/ulid/v2"
	"github.com/slatedb/slatedb-go/slatedb"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/thanos-io/objstore"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Choice struct {
	Name  string
	Value interface{}
}

func (c *Choice) GetName() string {
	return c.Name
}
func (c *Choice) GetValue() interface{} {
	return c.Value
}

type Role interface {
	GetState() (map[string]interface{}, error)
}

type ObjectStoreRole interface {
	Role
}
type ObjectStoreRoleAdapter struct {
	bucket *objstore.InMemBucket
	db     *slatedb.DB
}

// assert that SlateDbRoleAdapter satisfies SlateDbRole
var _ ObjectStoreRole = (*ObjectStoreRoleAdapter)(nil)

func (r *ObjectStoreRoleAdapter) GetState() (map[string]interface{}, error) {

	objects := make(map[string]interface{})
	for k, _ := range r.bucket.Objects() {
		if strings.HasPrefix(k, "wal/") {
			baseName := extractBaseName(k)
			sstId, err := strconv.Atoi(baseName)
			if err != nil {
				panic(err)
			}
			data, err := slatedb.WalSstToMap(r.db, uint64(sstId))
			if err != nil {
				return nil, err
			}
			objects[fmt.Sprintf("wal/%04d.sst", sstId)] = data
		} else if strings.HasPrefix(k, "compacted/") {
			baseName := extractBaseName(k)
			sstId, err := ulid.Parse(baseName)
			if err != nil {
				return nil, err
			}

			data, err := slatedb.CompactedSstToMap(r.db, sstId)
			if err != nil {
				return nil, err
			}
			objects[fmt.Sprintf("compacted/ulid-%04d.sst", sstId.Time())] = data
		}
	}
	return map[string]interface{}{"objects": objects}, nil
}

type SlateDbRole interface {
	Role
	Put(choices []Choice) (interface{}, error)
	Get(choices []Choice) (interface{}, error)
	FlushWal(choices []Choice) (interface{}, error)
	FlushMemtable(choices []Choice) (interface{}, error)
}

type SlateDbRoleAdapter struct {
	db *slatedb.DB
}

// assert that SlateDbRoleAdapter satisfies SlateDbRole
var _ SlateDbRole = (*SlateDbRoleAdapter)(nil)

func (r *SlateDbRoleAdapter) GetState() (map[string]interface{}, error) {
	return slatedb.GetState(r.db)
}

func (r *SlateDbRoleAdapter) Put(choices []Choice) (interface{}, error) {
	key := []byte(choices[0].GetValue().(string))
	value := []byte(choices[1].GetValue().(string))

	writeOptions := slatedb.DefaultWriteOptions()
	writeOptions.AwaitDurable = false
	if choices[1].GetValue().(string) == "notfound" {
		r.db.DeleteWithOptions(key, writeOptions)
	} else {
		r.db.PutWithOptions(key, value, writeOptions)
	}

	return nil, nil
}

func (r *SlateDbRoleAdapter) Get(choices []Choice) (interface{}, error) {
	key := []byte(choices[0].GetValue().(string))
	level := choices[1].GetValue().(string)
	readOptions := slatedb.DefaultReadOptions()
	if level == "Uncommitted" {
		readOptions = slatedb.ReadOptions{ReadLevel: slatedb.Uncommitted}
	}
	val, err := r.db.GetWithOptions(context.Background(), key, readOptions)
	if err != nil {
		if errors.Is(err, common.ErrKeyNotFound) {
			return "notfound", nil
		}
		return nil, err
	}
	return string(val), nil
}

func (r *SlateDbRoleAdapter) FlushWal(_ []Choice) (interface{}, error) {
	//fmt.Println("Calling: db.FlushWal()")
	return nil, r.db.FlushWAL()
}

func (r *SlateDbRoleAdapter) FlushMemtable(_ []Choice) (interface{}, error) {
	//fmt.Println("Calling: db.FlushMemtableToL0()")
	return nil, r.db.FlushMemtableToL0()
}

type Model struct {
	Roles map[string]Role        `json:"roles"`
	State map[string]interface{} `json:"state"`
}

func NewModel() any {
	return &Model{}
}

func (m *Model) ToJson() string {
	// json marshall to string
	bytes, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

var ulidTimeMs atomic.Uint64

func init() {
	gomonkey.ApplyFunc(ulid.Make, func() ulid.ULID {
		newTimeMs := ulidTimeMs.Add(1)
		return ulid.MustNew(newTimeMs, nil)
	})
}
func (m *Model) Init() {
	ulidTimeMs.Store(0)
	bucket := objstore.NewInMemBucket()
	dbOptions := slatedb.DefaultDBOptions()
	dbOptions.FlushInterval = 10 * time.Minute
	dbOptions.CompactorOptions.PollInterval = 10 * time.Minute

	db, _ := slatedb.OpenWithOptions(context.Background(), "", bucket, dbOptions)

	writer := &SlateDbRoleAdapter{db}
	store := &ObjectStoreRoleAdapter{bucket: bucket, db: db}

	m.State = make(map[string]interface{})
	m.State["store"] = store
	m.State["writer"] = writer

	m.Roles = make(map[string]Role)
	m.Roles["ObjectStore#0"] = store
	m.Roles["SlateDb#0"] = writer
}

func (m *Model) InternalCleanup() {
	m.State["writer"].(*SlateDbRoleAdapter).db.Close()
}

// Function to extract base32 string from "compacted/somebase32string.sst"
func extractBaseName(path string) string {
	parts := strings.Split(path, "/")
	fileName := parts[len(parts)-1]
	return strings.TrimSuffix(fileName, ".sst")
}

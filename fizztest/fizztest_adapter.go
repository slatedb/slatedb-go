package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/oklog/ulid/v2"
	"github.com/slatedb/slatedb-go/slatedb"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"strconv"
	"strings"
	"testing"
	"time"
)

type Choice struct {
	Name  string
	Value interface{}
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
	key := []byte(choices[0].Value.(string))
	value := []byte(choices[1].Value.(string))

	writeOptions := slatedb.DefaultWriteOptions()
	writeOptions.AwaitFlush = false
	if choices[1].Value.(string) == "notfound" {
		r.db.DeleteWithOptions(key, writeOptions)
	} else {
		r.db.PutWithOptions(key, value, writeOptions)
	}

	return nil, nil
}

func (r *SlateDbRoleAdapter) Get(choices []Choice) (interface{}, error) {
	key := []byte(choices[0].Value.(string))
	level := choices[1].Value.(string)
	readOptions := slatedb.DefaultReadOptions()
	if level == "Uncommitted" {
		readOptions = slatedb.ReadOptions{ReadLevel: slatedb.Uncommitted}
	}
	val, err := r.db.GetWithOptions(key, readOptions)
	if err != nil {
		if errors.Is(err, common.ErrKeyNotFound) {
			return "notfound", nil
		}
		return nil, err
	}
	return string(val), nil
}

func (r *SlateDbRoleAdapter) FlushWal(_ []Choice) (interface{}, error) {
	return nil, r.db.FlushWAL()
}

func (r *SlateDbRoleAdapter) FlushMemtable(_ []Choice) (interface{}, error) {
	return nil, r.db.FlushMemtableToL0()
}

type Model struct {
	Roles map[string]Role        `json:"roles"`
	State map[string]interface{} `json:"state"`

	patches *gomonkey.Patches
}

func (m *Model) ToJson() string {
	// json marshall to string
	bytes, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func (m *Model) Init() {
	timeMs := uint64(0)
	m.patches = gomonkey.ApplyFunc(ulid.Make, func() ulid.ULID {
		timeMs++
		return ulid.MustNew(timeMs, nil)
	})

	bucket := objstore.NewInMemBucket()
	dbOptions := slatedb.DefaultDBOptions()
	dbOptions.FlushInterval = 10 * time.Minute
	dbOptions.CompactorOptions.PollInterval = 10 * time.Minute

	db, _ := slatedb.OpenWithOptions("", bucket, dbOptions)

	writer := &SlateDbRoleAdapter{db}
	store := &ObjectStoreRoleAdapter{bucket: bucket, db: db}

	m.State = make(map[string]interface{})
	m.State["store"] = store
	m.State["writer"] = writer
	m.State["next_ulid"] = func() int { return int(timeMs) + 1 }

	m.Roles = make(map[string]Role)
	m.Roles["ObjectStore#0"] = store
	m.Roles["SlateDb#0"] = writer
}

func (m *Model) InternalCleanup() {
	m.patches.Reset()
	m.State["writer"].(*SlateDbRoleAdapter).db.Close()
}

func AssertModelEquals(t *testing.T, exp string, model *Model, retVal interface{}) {
	var node map[string]interface{}
	if err := json.Unmarshal([]byte(exp), &node); err != nil {
		panic(err)
	}

	expectedRolesMap := make(map[string]map[string]interface{})
	if node["roles"] != nil {
		expectedRoles := node["roles"].([]interface{})
		for _, r := range expectedRoles {
			role := r.(map[string]interface{})
			expectedRolesMap[role["ref_string"].(string)] = role["fields"].(map[string]interface{})
		}
	}

	for roleRef, role := range model.Roles {
		state, err := role.GetState()
		if err != nil {
			panic(err)
		}
		e, ok := expectedRolesMap[roleRef]

		require.True(t, ok)
		assert.Equal(t, e, state)
	}
}

// Function to extract base32 string from "compacted/somebase32string.sst"
func extractBaseName(path string) string {
	parts := strings.Split(path, "/")
	fileName := parts[len(parts)-1]
	return strings.TrimSuffix(fileName, ".sst")
}

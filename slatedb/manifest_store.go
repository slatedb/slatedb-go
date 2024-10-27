package slatedb

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/logger"
	"github.com/thanos-io/objstore"
	"go.uber.org/zap"
)

const manifestDir = "manifest"

type EpochType int

const (
	WriterEpoch EpochType = iota + 1
	CompactorEpoch
)

// ------------------------------------------------
// FenceableManifest
// ------------------------------------------------

// FenceableManifest wraps StoredManifest, and fences other conflicting writers by incrementing
// the relevant epoch when initialized. It also detects when the current writer has been
// fenced and fails all operations with error ErrFenced.
type FenceableManifest struct {
	storedManifest *StoredManifest
	localEpoch     atomic.Uint64
	epochType      EpochType
}

func initFenceableManifestWriter(storedManifest *StoredManifest) (*FenceableManifest, error) {
	manifest := storedManifest.manifest
	manifest.writerEpoch.Add(1)
	err := storedManifest.updateManifest(manifest)
	if err != nil {
		return nil, err
	}

	fm := &FenceableManifest{
		storedManifest: storedManifest,
		epochType:      WriterEpoch,
	}
	fm.localEpoch.Store(manifest.writerEpoch.Load())
	return fm, nil
}

func initFenceableManifestCompactor(storedManifest *StoredManifest) (*FenceableManifest, error) {
	manifest := storedManifest.manifest
	manifest.compactorEpoch.Add(1)
	err := storedManifest.updateManifest(manifest)
	if err != nil {
		return nil, err
	}

	fm := &FenceableManifest{
		storedManifest: storedManifest,
		epochType:      CompactorEpoch,
	}
	fm.localEpoch.Store(manifest.compactorEpoch.Load())
	return fm, nil
}

func (f *FenceableManifest) dbState() (*CoreDBState, error) {
	err := f.checkEpoch()
	if err != nil {
		logger.Error("unable to get db state", zap.Error(err))
		return nil, err
	}
	return f.storedManifest.dbState(), nil
}

func (f *FenceableManifest) updateDBState(dbState *CoreDBState) error {
	err := f.checkEpoch()
	if err != nil {
		return err
	}
	return f.storedManifest.updateDBState(dbState)
}

func (f *FenceableManifest) refresh() (*CoreDBState, error) {
	_, err := f.storedManifest.refresh()
	if err != nil {
		return nil, err
	}
	return f.dbState()
}

func (f *FenceableManifest) storedEpoch() uint64 {
	if f.epochType == WriterEpoch {
		return f.storedManifest.manifest.writerEpoch.Load()
	} else {
		return f.storedManifest.manifest.compactorEpoch.Load()
	}
}

func (f *FenceableManifest) checkEpoch() error {
	if f.localEpoch.Load() < f.storedEpoch() {
		logger.Warn("detenced newer client")
		return common.ErrFenced
	}
	if f.localEpoch.Load() > f.storedEpoch() {
		panic("the stored epoch is lower than the local epoch")
	}
	return nil
}

// ------------------------------------------------
// StoredManifest
// ------------------------------------------------

// StoredManifest Represents the manifest stored in the object store. This type tracks the current
// contents and id of the stored manifest, and allows callers to read the db state
// stored therein. Callers can also use this type to update the db state stored in the
// manifest. The update is done with the next consecutive id, and is conditional on
// no other writer having made an update to the manifest using that id. Finally, callers
// can use the `refresh` method to refresh the locally stored manifest+id with the latest
// manifest stored in the object store.
type StoredManifest struct {
	id            uint64
	manifest      *Manifest
	manifestStore *ManifestStore
}

func newStoredManifest(store *ManifestStore, core *CoreDBState) (*StoredManifest, error) {
	manifest := &Manifest{
		core: core,
	}
	err := store.writeManifest(1, manifest)
	if err != nil {
		logger.Error("unable to store manifest", zap.Error(err))
		return nil, err
	}

	return &StoredManifest{
		id:            1,
		manifest:      manifest,
		manifestStore: store,
	}, nil
}

func loadStoredManifest(store *ManifestStore) (mo.Option[StoredManifest], error) {
	stored, err := store.readLatestManifest()
	if err != nil {
		logger.Error("unable to read latest manifest", zap.Error(err))
		return mo.None[StoredManifest](), err
	}
	if stored.IsAbsent() {
		return mo.None[StoredManifest](), nil
	}
	storedInfo, _ := stored.Get()
	return mo.Some(StoredManifest{
		id:            storedInfo.id,
		manifest:      storedInfo.manifest,
		manifestStore: store,
	}), nil
}

func (s *StoredManifest) dbState() *CoreDBState {
	return s.manifest.core
}

func (s *StoredManifest) updateDBState(core *CoreDBState) error {
	manifest := &Manifest{
		core: core,
	}
	manifest.writerEpoch.Store(s.manifest.writerEpoch.Load())
	manifest.compactorEpoch.Store(s.manifest.compactorEpoch.Load())
	return s.updateManifest(manifest)
}

func (s *StoredManifest) updateManifest(manifest *Manifest) error {
	newID := s.id + 1
	err := s.manifestStore.writeManifest(newID, manifest)
	if err != nil {
		logger.Error("unable to write store manifest", zap.Error(err))
		return err
	}
	s.manifest = manifest
	s.id = newID
	return nil
}

func (s *StoredManifest) refresh() (*CoreDBState, error) {
	stored, err := s.manifestStore.readLatestManifest()
	if err != nil {
		logger.Error("unable to load latest manifest", zap.Error(err))
		return nil, err
	}
	if stored.IsAbsent() {
		return nil, common.ErrInvalidDBState
	}

	storedInfo, _ := stored.Get()
	s.manifest = storedInfo.manifest
	s.id = storedInfo.id
	return s.manifest.core, nil
}

// ------------------------------------------------
// ManifestStore
// ------------------------------------------------

type ManifestStore struct {
	objectStore    ObjectStore
	codec          ManifestCodec
	manifestSuffix string
}

func newManifestStore(rootPath string, bucket objstore.Bucket) *ManifestStore {
	return &ManifestStore{
		objectStore:    newDelegatingObjectStore(rootPath, bucket),
		codec:          FlatBufferManifestCodec{},
		manifestSuffix: "manifest",
	}
}

func (s *ManifestStore) manifestPath(id uint64) string {
	filepath := fmt.Sprintf("%020d.%s", id, s.manifestSuffix)
	return path.Join(manifestDir, filepath)
}

func (s *ManifestStore) writeManifest(id uint64, manifest *Manifest) error {
	filepath := s.manifestPath(id)
	err := s.objectStore.putIfNotExists(filepath, s.codec.encode(manifest))
	if err != nil {
		logger.Error("failed to complete the operation", zap.Error(err))
		if errors.Is(err, common.ErrObjectExists) {
			return common.ErrManifestVersionExists
		}
		return common.ErrObjectStore
	}
	return nil
}

type manifestInfo struct {
	id       uint64
	manifest *Manifest
}

func (s *ManifestStore) readLatestManifest() (mo.Option[manifestInfo], error) {
	files, err := s.objectStore.list(mo.Some(manifestDir))
	if err != nil {
		logger.Error("unable to list manifest files", zap.Error(err))
		return mo.None[manifestInfo](), common.ErrObjectStore
	}

	latestManifestPath := ""
	latestManifestID := uint64(0)
	// This loop will search for the manifest with the highest ID
	// (which is the latest manifest)
	for _, filepath := range files {
		foundID, err := s.parseID(filepath, "."+s.manifestSuffix)
		if err != nil {
			logger.Error("unable to parse manifest file", zap.Error(err))
			continue
		}

		if latestManifestPath == "" {
			latestManifestID = foundID
			latestManifestPath = filepath
			continue
		}

		if foundID > latestManifestID {
			latestManifestID = foundID
			latestManifestPath = filepath
		}
	}
	if latestManifestPath == "" {
		return mo.None[manifestInfo](), nil
	}

	// read the latest manifest from object store and return the manifest
	manifestBytes, err := s.objectStore.get(path.Join(manifestDir, basePath(latestManifestPath)))
	if err != nil {
		logger.Error("unable to read latest manifest from the store", zap.Error(err))
		return mo.None[manifestInfo](), common.ErrObjectStore
	}

	manifest, err := s.codec.decode(manifestBytes)
	if err != nil {
		logger.Error("unable to decode manifest", zap.Error(err))
		return mo.None[manifestInfo](), err
	}
	return mo.Some(manifestInfo{latestManifestID, manifest}), nil
}

func (s *ManifestStore) parseID(filepath string, expectedExt string) (uint64, error) {
	if path.Ext(filepath) != expectedExt {
		logger.Warn("invalid file extension")
		return 0, common.ErrInvalidDBState
	}

	base := path.Base(filepath)
	idStr := strings.Replace(base, expectedExt, "", 1)
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		logger.Error("invalid id", zap.Error(err))
		return 0, common.ErrInvalidDBState
	}

	return id, nil
}

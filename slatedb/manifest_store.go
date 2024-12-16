package slatedb

import (
	"cmp"
	"errors"
	"fmt"
	"github.com/samber/mo"
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/thanos-io/objstore"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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

// write Manifest with updated DB state to object store and update StoredManifest with the new manifest
func (s *StoredManifest) updateDBState(core *CoreDBState) error {
	manifest := &Manifest{
		core: core,
	}
	manifest.writerEpoch.Store(s.manifest.writerEpoch.Load())
	manifest.compactorEpoch.Store(s.manifest.compactorEpoch.Load())
	return s.updateManifest(manifest)
}

// write given manifest to object store and update StoredManifest with given manifest
func (s *StoredManifest) updateManifest(manifest *Manifest) error {
	newID := s.id + 1
	err := s.manifestStore.writeManifest(newID, manifest)
	if err != nil {
		return err
	}
	s.manifest = manifest
	s.id = newID
	return nil
}

// read latest manifest from object store and update StoredManifest with the latest manifest.
func (s *StoredManifest) refresh() (*CoreDBState, error) {
	stored, err := s.manifestStore.readLatestManifest()
	if err != nil {
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

type ManifestFileMetadata struct {
	ID uint64

	// LastModified is the timestamp the manifest was last modified.
	LastModified time.Time

	// Location is the path of the manifest
	Location string
}

type manifestInfo struct {
	id       uint64
	manifest *Manifest
}

// ManifestStore has helper methods to read and write manifest to object store
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

func (s *ManifestStore) manifestPath(filename string) string {
	return path.Join(manifestDir, filename)
}

func (s *ManifestStore) writeManifest(id uint64, manifest *Manifest) error {
	filepath := s.manifestPath(fmt.Sprintf("%020d.%s", id, s.manifestSuffix))
	err := s.objectStore.putIfNotExists(filepath, s.codec.encode(manifest))
	if err != nil {
		if errors.Is(err, common.ErrObjectExists) {
			return common.ErrManifestVersionExists
		}
		return common.ErrObjectStore
	}
	return nil
}

func (s *ManifestStore) listManifests() ([]ManifestFileMetadata, error) {
	objMetaList, err := s.objectStore.list(mo.Some(manifestDir))
	if err != nil {
		return nil, common.ErrObjectStore
	}

	manifests := make([]ManifestFileMetadata, 0)
	for _, objMeta := range objMetaList {
		id, err := s.parseID(objMeta.Location, "."+s.manifestSuffix)
		if err != nil {
			continue
		}

		manifests = append(manifests, ManifestFileMetadata{
			ID:           id,
			LastModified: objMeta.LastModified,
			Location:     objMeta.Location,
		})
	}

	slices.SortFunc(manifests, func(a, b ManifestFileMetadata) int {
		return cmp.Compare(a.ID, b.ID)
	})
	return manifests, nil
}

func (s *ManifestStore) readLatestManifest() (mo.Option[manifestInfo], error) {
	manifestList, err := s.listManifests()
	if err != nil || len(manifestList) == 0 {
		return mo.None[manifestInfo](), err
	}

	latestManifest := manifestList[len(manifestList)-1]
	if latestManifest.Location == "" {
		return mo.None[manifestInfo](), nil
	}

	// read the latest manifest from object store and return the manifest
	filename := path.Base(latestManifest.Location)
	manifestBytes, err := s.objectStore.get(s.manifestPath(filename))
	if err != nil {
		return mo.None[manifestInfo](), err
	}

	manifest, err := s.codec.decode(manifestBytes)
	if err != nil {
		return mo.None[manifestInfo](), err
	}
	return mo.Some(manifestInfo{latestManifest.ID, manifest}), nil
}

func (s *ManifestStore) parseID(filepath string, expectedExt string) (uint64, error) {
	if path.Ext(filepath) != expectedExt {
		return 0, common.ErrInvalidDBState
	}

	base := path.Base(filepath)
	idStr := strings.Replace(base, expectedExt, "", 1)
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return 0, common.ErrInvalidDBState
	}

	return id, nil
}

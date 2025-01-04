package store

import (
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/slatedb/slatedb-go/slatedb/state"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"testing"
)

func TestShouldFailWriteOnVersionConflict(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := NewManifestStore(rootPath, bucket)
	coreState := state.NewCoreDBState()

	sm, err := NewStoredManifest(manifestStore, coreState)
	assert.NoError(t, err)

	storedManifest, err := LoadStoredManifest(manifestStore)
	assert.NoError(t, err)
	sm2, ok := storedManifest.Get()
	assert.True(t, ok)

	err = sm.updateDBState(coreState.Snapshot())
	assert.NoError(t, err)

	err = sm2.updateDBState(coreState.Snapshot())
	assert.ErrorIs(t, err, common.ErrManifestVersionExists)
}

func TestShouldWriteWithNewVersion(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := NewManifestStore(rootPath, bucket)
	coreState := state.NewCoreDBState()

	sm, err := NewStoredManifest(manifestStore, coreState)
	assert.NoError(t, err)

	err = sm.updateDBState(coreState.Snapshot())
	assert.NoError(t, err)

	info, err := manifestStore.readLatestManifest()
	assert.NoError(t, err)
	assert.True(t, info.IsPresent())

	manifest, _ := info.Get()
	assert.Equal(t, uint64(2), manifest.id)
}

func TestShouldUpdateLocalStateOnWrite(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := NewManifestStore(rootPath, bucket)
	coreState := state.NewCoreDBState()

	sm, err := NewStoredManifest(manifestStore, coreState)
	assert.NoError(t, err)

	core := coreState.Snapshot()
	core.NextWalSstID.Store(123)
	err = sm.updateDBState(core)
	assert.NoError(t, err)
	assert.Equal(t, uint64(123), sm.DbState().NextWalSstID.Load())
}

func TestShouldRefresh(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := NewManifestStore(rootPath, bucket)
	coreState := state.NewCoreDBState()

	sm, err := NewStoredManifest(manifestStore, coreState)
	assert.NoError(t, err)

	storedManifest, err := LoadStoredManifest(manifestStore)
	assert.NoError(t, err)
	sm2, ok := storedManifest.Get()
	assert.True(t, ok)

	core := coreState.Snapshot()
	core.NextWalSstID.Store(123)
	err = sm.updateDBState(core)
	assert.NoError(t, err)

	refreshed, err := sm2.Refresh()
	assert.NoError(t, err)
	assert.Equal(t, uint64(123), refreshed.NextWalSstID.Load())
	assert.Equal(t, uint64(123), sm.DbState().NextWalSstID.Load())
}

func TestShouldBumpWriterEpoch(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := NewManifestStore(rootPath, bucket)
	coreState := state.NewCoreDBState()

	_, err := NewStoredManifest(manifestStore, coreState)
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		storedManifest, err := LoadStoredManifest(manifestStore)
		assert.NoError(t, err)
		sm, ok := storedManifest.Get()
		assert.True(t, ok)

		_, err = InitFenceableManifestWriter(&sm)
		assert.NoError(t, err)

		info, err := manifestStore.readLatestManifest()
		assert.NoError(t, err)
		assert.True(t, info.IsPresent())
		mInfo, _ := info.Get()
		assert.Equal(t, uint64(i), mInfo.manifest.WriterEpoch.Load())
	}
}

func TestShouldFailOnWriterFenced(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := NewManifestStore(rootPath, bucket)
	coreState := state.NewCoreDBState()

	sm, err := NewStoredManifest(manifestStore, coreState)
	assert.NoError(t, err)
	writer1, err := InitFenceableManifestWriter(sm)
	assert.NoError(t, err)

	storedManifest, err := LoadStoredManifest(manifestStore)
	assert.NoError(t, err)
	sm2, ok := storedManifest.Get()
	assert.True(t, ok)
	writer2, err := InitFenceableManifestWriter(&sm2)
	assert.NoError(t, err)

	_, err = writer1.Refresh()
	assert.ErrorIs(t, err, common.ErrFenced)
	core := coreState.Snapshot()
	core.NextWalSstID.Store(123)
	err = writer1.UpdateDBState(core)
	assert.ErrorIs(t, err, common.ErrFenced)

	refreshed, err := writer2.Refresh()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), refreshed.NextWalSstID.Load())
}

func TestShouldBumpCompactorEpoch(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := NewManifestStore(rootPath, bucket)
	coreState := state.NewCoreDBState()

	_, err := NewStoredManifest(manifestStore, coreState)
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		storedManifest, err := LoadStoredManifest(manifestStore)
		assert.NoError(t, err)
		sm, ok := storedManifest.Get()
		assert.True(t, ok)

		_, err = InitFenceableManifestCompactor(&sm)
		assert.NoError(t, err)

		info, err := manifestStore.readLatestManifest()
		assert.NoError(t, err)
		assert.True(t, info.IsPresent())
		mInfo, _ := info.Get()
		assert.Equal(t, uint64(i), mInfo.manifest.CompactorEpoch.Load())
	}
}

func TestShouldFailOnCompactorFenced(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := NewManifestStore(rootPath, bucket)
	coreState := state.NewCoreDBState()

	sm, err := NewStoredManifest(manifestStore, coreState)
	assert.NoError(t, err)
	compactor1, err := InitFenceableManifestCompactor(sm)
	assert.NoError(t, err)

	storedManifest, err := LoadStoredManifest(manifestStore)
	assert.NoError(t, err)
	sm2, ok := storedManifest.Get()
	assert.True(t, ok)
	compactor2, err := InitFenceableManifestCompactor(&sm2)
	assert.NoError(t, err)

	_, err = compactor1.Refresh()
	assert.ErrorIs(t, err, common.ErrFenced)
	core := coreState.Snapshot()
	core.NextWalSstID.Store(123)
	err = compactor1.UpdateDBState(core)
	assert.ErrorIs(t, err, common.ErrFenced)

	refreshed, err := compactor2.Refresh()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), refreshed.NextWalSstID.Load())
}

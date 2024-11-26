package slatedb

import (
	"github.com/slatedb/slatedb-go/slatedb/common"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/objstore"
	"testing"
)

func TestShouldFailWriteOnVersionConflict(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := newManifestStore(rootPath, bucket)
	state := newCoreDBState()

	sm, err := newStoredManifest(manifestStore, state)
	assert.NoError(t, err)

	storedManifest, err := loadStoredManifest(manifestStore)
	assert.NoError(t, err)
	sm2, ok := storedManifest.Get()
	assert.True(t, ok)

	err = sm.updateDBState(state)
	assert.NoError(t, err)

	err = sm2.updateDBState(state)
	assert.ErrorIs(t, err, common.ErrManifestVersionExists)
}

func TestShouldWriteWithNewVersion(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := newManifestStore(rootPath, bucket)
	state := newCoreDBState()

	sm, err := newStoredManifest(manifestStore, state)
	assert.NoError(t, err)

	err = sm.updateDBState(state)
	assert.NoError(t, err)

	info, err := manifestStore.readLatestManifest()
	assert.NoError(t, err)
	assert.True(t, info.IsPresent())

	manifest, _ := info.Get()
	assert.Equal(t, uint64(2), manifest.id)
}

func TestShouldUpdateLocalStateOnWrite(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := newManifestStore(rootPath, bucket)
	state := newCoreDBState()

	sm, err := newStoredManifest(manifestStore, state)
	assert.NoError(t, err)

	state.nextWalSstID.Store(123)
	err = sm.updateDBState(state)
	assert.NoError(t, err)
	assert.Equal(t, uint64(123), sm.dbState().nextWalSstID.Load())
}

func TestShouldRefresh(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := newManifestStore(rootPath, bucket)
	state := newCoreDBState()

	sm, err := newStoredManifest(manifestStore, state)
	assert.NoError(t, err)

	storedManifest, err := loadStoredManifest(manifestStore)
	assert.NoError(t, err)
	sm2, ok := storedManifest.Get()
	assert.True(t, ok)

	state.nextWalSstID.Store(123)
	err = sm.updateDBState(state)
	assert.NoError(t, err)

	refreshed, err := sm2.refresh()
	assert.NoError(t, err)
	assert.Equal(t, uint64(123), refreshed.nextWalSstID.Load())
	assert.Equal(t, uint64(123), sm.dbState().nextWalSstID.Load())
}

func TestShouldBumpWriterEpoch(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := newManifestStore(rootPath, bucket)
	state := newCoreDBState()

	_, err := newStoredManifest(manifestStore, state)
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		storedManifest, err := loadStoredManifest(manifestStore)
		assert.NoError(t, err)
		sm, ok := storedManifest.Get()
		assert.True(t, ok)

		_, err = initFenceableManifestWriter(&sm)
		assert.NoError(t, err)

		info, err := manifestStore.readLatestManifest()
		assert.NoError(t, err)
		assert.True(t, info.IsPresent())
		mInfo, _ := info.Get()
		assert.Equal(t, uint64(i), mInfo.manifest.writerEpoch.Load())
	}
}

func TestShouldFailOnWriterFenced(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := newManifestStore(rootPath, bucket)
	state := newCoreDBState()

	sm, err := newStoredManifest(manifestStore, state)
	assert.NoError(t, err)
	writer1, err := initFenceableManifestWriter(sm)
	assert.NoError(t, err)

	storedManifest, err := loadStoredManifest(manifestStore)
	assert.NoError(t, err)
	sm2, ok := storedManifest.Get()
	assert.True(t, ok)
	writer2, err := initFenceableManifestWriter(&sm2)
	assert.NoError(t, err)

	_, err = writer1.refresh()
	assert.ErrorIs(t, err, common.ErrFenced)
	state.nextWalSstID.Store(123)
	err = writer1.updateDBState(state)
	assert.ErrorIs(t, err, common.ErrFenced)

	refreshed, err := writer2.refresh()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), refreshed.nextWalSstID.Load())
}

func TestShouldBumpCompactorEpoch(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := newManifestStore(rootPath, bucket)
	state := newCoreDBState()

	_, err := newStoredManifest(manifestStore, state)
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		storedManifest, err := loadStoredManifest(manifestStore)
		assert.NoError(t, err)
		sm, ok := storedManifest.Get()
		assert.True(t, ok)

		_, err = initFenceableManifestCompactor(&sm)
		assert.NoError(t, err)

		info, err := manifestStore.readLatestManifest()
		assert.NoError(t, err)
		assert.True(t, info.IsPresent())
		mInfo, _ := info.Get()
		assert.Equal(t, uint64(i), mInfo.manifest.compactorEpoch.Load())
	}
}

func TestShouldFailOnCompactorFenced(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	manifestStore := newManifestStore(rootPath, bucket)
	state := newCoreDBState()

	sm, err := newStoredManifest(manifestStore, state)
	assert.NoError(t, err)
	compactor1, err := initFenceableManifestCompactor(sm)
	assert.NoError(t, err)

	storedManifest, err := loadStoredManifest(manifestStore)
	assert.NoError(t, err)
	sm2, ok := storedManifest.Get()
	assert.True(t, ok)
	compactor2, err := initFenceableManifestCompactor(&sm2)
	assert.NoError(t, err)

	_, err = compactor1.refresh()
	assert.ErrorIs(t, err, common.ErrFenced)
	state.nextWalSstID.Store(123)
	err = compactor1.updateDBState(state)
	assert.ErrorIs(t, err, common.ErrFenced)

	refreshed, err := compactor2.refresh()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), refreshed.nextWalSstID.Load())
}

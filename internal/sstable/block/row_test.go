package block

import (
	"bytes"
	"github.com/kapetan-io/tackle/random"
	"github.com/slatedb/slatedb-go/internal/compress"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRowFlags(t *testing.T) {
	tests := []struct {
		name     string
		row      Row
		expected v0RowFlags
	}{
		{
			name:     "Empty row",
			row:      Row{},
			expected: 0,
		},
		{
			name: "Tombstone",
			row: Row{
				Value: types.Value{Kind: types.KindTombStone},
			},
			expected: flagTombstone,
		},
		{
			name: "WithExpire",
			row: Row{
				ExpireAt: time.Now(),
			},
			expected: flagHasExpire,
		},
		{
			name: "WithCreate",
			row: Row{
				CreatedAt: time.Now(),
			},
			expected: flagHasCreate,
		},
		{
			name: "AllFlags",
			row: Row{
				Value:     types.Value{Kind: types.KindTombStone},
				ExpireAt:  time.Now(),
				CreatedAt: time.Now(),
			},
			expected: flagTombstone | flagHasExpire | flagHasCreate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, v0Flags(tt.row))
		})
	}
}

func TestV0RowCodecDecodeErrors(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expectedErr string
	}{
		{
			name:        "TooShort",
			input:       []byte{0, 1, 2},
			expectedErr: v0ErrPrefix + "data length too short to decode a row",
		},
		{
			name:        "InvalidKeySuffixLength",
			input:       []byte{0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: v0ErrPrefix + "key suffix length exceeds length of block",
		},
		{
			name:        "InvalidKeyPrefixLength",
			input:       []byte{0, 255, 0, 1, 23, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: v0ErrPrefix + "key prefix length exceeds length of first key in block",
		},
		{
			name:        "InvalidExpireTimestamp",
			input:       []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
			expectedErr: v0ErrPrefix + "data length too short for expire",
		},
		{
			name:        "InvalidCreateTimestamp",
			input:       []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4},
			expectedErr: v0ErrPrefix + "data length too short for create",
		},
		{
			name:        "InvalidValueLength",
			input:       []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: v0ErrPrefix + "data length too short for for value length",
		},
		{
			name:        "InvalidValue",
			input:       []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5},
			expectedErr: v0ErrPrefix + "data length too short for for value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v0RowCodec.Decode(tt.input, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestV0CodecPeekAtKeyErrors(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expectedErr string
	}{
		{
			name:        "TooShort",
			input:       []byte{0, 1, 2},
			expectedErr: v0ErrPrefix + "data length too short to peek at row",
		},
		{
			name:        "InvalidKeySuffixLength",
			input:       []byte{0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: v0ErrPrefix + "key suffix length exceeds length of block",
		},
		{
			name:        "InvalidKeyPrefixLength",
			input:       []byte{0, 255, 0, 1, 23, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: v0ErrPrefix + "key prefix length exceeds length of first key in block",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v0RowCodec.PeekAtKey(tt.input, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestRowCodecV0EncodeAndDecode(t *testing.T) {
	tests := []struct {
		name           string
		row            Row
		firstKeyPrefix []byte
	}{
		{
			name: "NormalRowWithExpireAt",
			row: Row{
				keyPrefixLen: 3,
				keySuffix:    []byte("key"),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.UnixMilli(10),
			},
			firstKeyPrefix: []byte("prefixdata"),
		},
		{
			name: "NormalRowWithoutExpireAt",
			row: Row{
				keyPrefixLen: 0,
				keySuffix:    []byte("key"),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte(""),
		},
		{
			name: "RowWithBothTimestamps",
			row: Row{
				keyPrefixLen: 5,
				keySuffix:    []byte("both"),
				Seq:          100,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.UnixMilli(1234567890),
				ExpireAt:     time.UnixMilli(9876543210),
			},
			firstKeyPrefix: []byte("test_both"),
		},
		{
			name: "RowWithOnlyCreateAt",
			row: Row{
				keyPrefixLen: 4,
				keySuffix:    []byte("create"),
				Seq:          50,
				Value:        types.Value{Value: []byte("test_value")},
				CreatedAt:    time.UnixMilli(1234567890),
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("timecreate"),
		},
		{
			name: "TombstoneRow",
			row: Row{
				keyPrefixLen: 4,
				keySuffix:    []byte("tomb"),
				Seq:          1,
				Value:        types.Value{Kind: types.KindTombStone},
				CreatedAt:    time.UnixMilli(2),
				ExpireAt:     time.UnixMilli(1),
			},
			firstKeyPrefix: []byte("deadbeefdata"),
		},
		{
			name: "EmptyKeySuffix",
			row: Row{
				keyPrefixLen: 4,
				keySuffix:    []byte(""),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("keyprefixdata"),
		},
		{
			name: "LargeSequenceNumber",
			row: Row{
				keyPrefixLen: 3,
				keySuffix:    []byte("seq"),
				Seq:          ^uint64(0),
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("bigseq"),
		},
		{
			name: "LargeValue",
			row: Row{
				keyPrefixLen: 2,
				keySuffix:    []byte("big"),
				Seq:          1,
				Value:        types.Value{Value: bytes.Repeat([]byte("x"), 100)},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("bigvalue"),
		},
		{
			name: "LongKeySuffix",
			row: Row{
				keyPrefixLen: 2,
				keySuffix:    bytes.Repeat([]byte("k"), 100),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("longkey"),
		},
		{
			name: "UnicodeKeySuffix",
			row: Row{
				keyPrefixLen: 3,
				keySuffix:    []byte("你好世界"),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("unicode"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the row
			encoded := v0RowCodec.Encode(tt.row)

			// Decode the row
			decoded, err := v0RowCodec.Decode(encoded, tt.firstKeyPrefix)
			require.NoError(t, err)
			assert.Equal(t, &tt.row, decoded)

			// Test restoring full key
			fullKey := v0FullKey(tt.row, tt.firstKeyPrefix)
			assert.Equal(t, append(tt.firstKeyPrefix[:tt.row.keyPrefixLen], decoded.keySuffix...), fullKey)
			//t.Logf("FullKey: %s", fullKey)
		})
	}
}

func TestComputePrefix(t *testing.T) {
	prefix := random.String("", 200)
	tests := []struct {
		name     string
		lhs      []byte
		rhs      []byte
		expected int
	}{
		{
			name:     "EmptySlices",
			lhs:      []byte{},
			rhs:      []byte{},
			expected: 0,
		},
		{
			name:     "NoCommonPrefix",
			lhs:      []byte{1, 2, 3},
			rhs:      []byte{4, 5, 6},
			expected: 0,
		},
		{
			name:     "PartialCommonPrefix",
			lhs:      []byte{1, 2, 3, 4},
			rhs:      []byte{1, 2, 5, 6},
			expected: 2,
		},
		{
			name:     "FullCommonPrefixDifferentLengths",
			lhs:      []byte{1, 2, 3},
			rhs:      []byte{1, 2, 3, 4, 5},
			expected: 3,
		},
		{
			name:     "ChunkOfCommonPrefix",
			lhs:      []byte(prefix + "with a common prefix"),
			rhs:      []byte(prefix + "with a different ending"),
			expected: 207,
		},
		{
			name:     "MultipleChunksOfCommonPrefix",
			lhs:      []byte(prefix + prefix + prefix + "with a common prefix"),
			rhs:      []byte(prefix + prefix + prefix + "with a different ending"),
			expected: 607,
		},
		{
			name:     "UnicodeStrings",
			lhs:      []byte("こんにちは世界"),
			rhs:      []byte("こんにちは地球"),
			expected: 15, // 5 characters, each 3 bytes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, uint16(tt.expected), computePrefixLen(tt.lhs, tt.rhs))
		})
	}
}

// goos: darwin
// goarch: amd64
// pkg: github.com/slatedb/slatedb-go/internal/sstable/block
// cpu: VirtualApple @ 2.50GHz
// BenchmarkComputePrefix
// BenchmarkComputePrefix/ShortStrings
// BenchmarkComputePrefix/ShortStrings-10         	173626988	         6.930 ns/op
// BenchmarkComputePrefix/MediumStrings
// BenchmarkComputePrefix/MediumStrings-10        	93353734	        12.52 ns/op
// BenchmarkComputePrefix/LongStrings
// BenchmarkComputePrefix/LongStrings-10          	21041848	        56.82 ns/op
// BenchmarkComputePrefix/NoCommonPrefix
// BenchmarkComputePrefix/NoCommonPrefix-10       	200386453	         5.970 ns/op
// BenchmarkComputePrefix/IdenticalStrings
// BenchmarkComputePrefix/IdenticalStrings-10     	97148622	        12.48 ns/op
// BenchmarkComputePrefix/EmptyStrings
// BenchmarkComputePrefix/EmptyStrings-10         	208189795	         5.808 ns/op
func BenchmarkComputePrefix(b *testing.B) {
	benchmarks := []struct {
		name string
		lhs  []byte
		rhs  []byte
	}{
		{
			name: "ShortStrings",
			lhs:  []byte("abc"),
			rhs:  []byte("abd"),
		},
		{
			name: "MediumStrings",
			lhs:  []byte("This is a medium length string"),
			rhs:  []byte("This is another medium length string"),
		},
		{
			name: "LongStrings",
			lhs:  []byte("This is a very long string that should test the performance with larger inputs"),
			rhs:  []byte("This is a very long string that should test the performance with larger things"),
		},
		{
			name: "NoCommonPrefix",
			lhs:  []byte("abcdef"),
			rhs:  []byte("ghijkl"),
		},
		{
			name: "IdenticalStrings",
			lhs:  []byte("identical"),
			rhs:  []byte("identical"),
		},
		{
			name: "EmptyStrings",
			lhs:  []byte{},
			rhs:  []byte{},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				computePrefixLen(bm.lhs, bm.rhs)
			}
		})
	}
}

func TestV0EstimateBlockSize(t *testing.T) {
	bb := NewBuilder(4096)
	assert.True(t, bb.IsEmpty())
	assert.True(t, bb.AddValue([]byte("k"), []byte("v")))
	assert.False(t, bb.IsEmpty())

	b, err := bb.Build()
	assert.NoError(t, err)
	blk, err := Encode(b, compress.CodecNone)
	assert.NoError(t, err)

	estimatedSize := V0EstimateBlockSize([]types.KeyValue{{Key: []byte("k"), Value: []byte("v")}})
	assert.Equal(t, uint64(len(blk)), estimatedSize)
}

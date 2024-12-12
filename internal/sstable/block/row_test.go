package block

import (
	"bytes"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRowFlags(t *testing.T) {
	tests := []struct {
		name     string
		row      v0Row
		expected v0RowFlags
	}{
		{
			name:     "Empty row",
			row:      v0Row{},
			expected: 0,
		},
		{
			name: "Tombstone",
			row: v0Row{
				Value: types.Value{Kind: types.KindTombStone},
			},
			expected: FlagTombstone,
		},
		{
			name: "WithExpire",
			row: v0Row{
				ExpireAt: time.Now(),
			},
			expected: FlagHasExpire,
		},
		{
			name: "WithCreate",
			row: v0Row{
				CreatedAt: time.Now(),
			},
			expected: FlagHasCreate,
		},
		{
			name: "AllFlags",
			row: v0Row{
				Value:     types.Value{Kind: types.KindTombStone},
				ExpireAt:  time.Now(),
				CreatedAt: time.Now(),
			},
			expected: FlagTombstone | FlagHasExpire | FlagHasCreate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.row.Flags())
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
			input:       []byte{0, 1, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: v0ErrPrefix + "data length too short for key suffix",
		},
		{
			name:        "InvalidExpireTimestamp",
			input:       []byte{0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
			expectedErr: v0ErrPrefix + "data length too short for expire",
		},
		{
			name:        "InvalidCreateTimestamp",
			input:       []byte{0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4},
			expectedErr: v0ErrPrefix + "data length too short for create",
		},
		{
			name:        "InvalidValueLength",
			input:       []byte{0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: v0ErrPrefix + "data length too short for for value length",
		},
		{
			name:        "InvalidValue",
			input:       []byte{0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5},
			expectedErr: v0ErrPrefix + "data length too short for for value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v0RowCodec.Decode(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestRowCodecV0EncodeAndDecode(t *testing.T) {
	tests := []struct {
		name           string
		row            v0Row
		firstKeyPrefix []byte
	}{
		{
			name: "NormalRowWithExpireAt",
			row: v0Row{
				KeyPrefixLen: 3,
				KeySuffix:    []byte("key"),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.UnixMilli(10),
			},
			firstKeyPrefix: []byte("prefixdata"),
		},
		{
			name: "NormalRowWithoutExpireAt",
			row: v0Row{
				KeyPrefixLen: 0,
				KeySuffix:    []byte("key"),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte(""),
		},
		{
			name: "RowWithBothTimestamps",
			row: v0Row{
				KeyPrefixLen: 5,
				KeySuffix:    []byte("both"),
				Seq:          100,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.UnixMilli(1234567890),
				ExpireAt:     time.UnixMilli(9876543210),
			},
			firstKeyPrefix: []byte("test_both"),
		},
		{
			name: "RowWithOnlyCreateAt",
			row: v0Row{
				KeyPrefixLen: 4,
				KeySuffix:    []byte("create"),
				Seq:          50,
				Value:        types.Value{Value: []byte("test_value")},
				CreatedAt:    time.UnixMilli(1234567890),
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("timecreate"),
		},
		{
			name: "TombstoneRow",
			row: v0Row{
				KeyPrefixLen: 4,
				KeySuffix:    []byte("tomb"),
				Seq:          1,
				Value:        types.Value{Kind: types.KindTombStone},
				CreatedAt:    time.UnixMilli(2),
				ExpireAt:     time.UnixMilli(1),
			},
			firstKeyPrefix: []byte("deadbeefdata"),
		},
		{
			name: "EmptyKeySuffix",
			row: v0Row{
				KeyPrefixLen: 4,
				KeySuffix:    []byte(""),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("keyprefixdata"),
		},
		{
			name: "LargeSequenceNumber",
			row: v0Row{
				KeyPrefixLen: 3,
				KeySuffix:    []byte("seq"),
				Seq:          ^uint64(0),
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("bigseq"),
		},
		{
			name: "LargeValue",
			row: v0Row{
				KeyPrefixLen: 2,
				KeySuffix:    []byte("big"),
				Seq:          1,
				Value:        types.Value{Value: bytes.Repeat([]byte("x"), 100)},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("bigvalue"),
		},
		{
			name: "LongKeySuffix",
			row: v0Row{
				KeyPrefixLen: 2,
				KeySuffix:    bytes.Repeat([]byte("k"), 100),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    time.Time{},
				ExpireAt:     time.Time{},
			},
			firstKeyPrefix: []byte("longkey"),
		},
		{
			name: "UnicodeKeySuffix",
			row: v0Row{
				KeyPrefixLen: 3,
				KeySuffix:    []byte("你好世界"),
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
			decoded, err := v0RowCodec.Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, &tt.row, decoded)

			// Test restoring full key
			fullKey := tt.row.FullKey(tt.firstKeyPrefix)
			assert.Equal(t, append(tt.firstKeyPrefix[:tt.row.KeyPrefixLen], decoded.KeySuffix...), fullKey)
			//t.Logf("FullKey: %s", fullKey)
		})
	}
}

func TestComputePrefix(t *testing.T) {
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
			name:     "LongCommonPrefix",
			lhs:      []byte("This is a long string with a common prefix"),
			rhs:      []byte("This is a long string with a different ending"),
			expected: 29,
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
			assert.Equal(t, uint16(tt.expected), computePrefix(tt.lhs, tt.rhs))
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
				computePrefix(bm.lhs, bm.rhs)
			}
		})
	}
}

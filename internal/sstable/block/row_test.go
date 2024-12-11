package block_test

import (
	"bytes"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"github.com/slatedb/slatedb-go/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRowFlags(t *testing.T) {
	tests := []struct {
		name     string
		row      block.Row
		expected block.RowFlags
	}{
		{
			name:     "Empty row",
			row:      block.Row{},
			expected: 0,
		},
		{
			name: "Tombstone",
			row: block.Row{
				Value: types.Value{Kind: types.KindTombStone},
			},
			expected: block.FlagTombstone,
		},
		{
			name: "With Expire",
			row: block.Row{
				ExpireAt: new(int64),
			},
			expected: block.FlagHasExpire,
		},
		{
			name: "With Create",
			row: block.Row{
				CreatedAt: new(int64),
			},
			expected: block.FlagHasCreate,
		},
		{
			name: "All flags",
			row: block.Row{
				Value:     types.Value{Kind: types.KindTombStone},
				ExpireAt:  new(int64),
				CreatedAt: new(int64),
			},
			expected: block.FlagTombstone | block.FlagHasExpire | block.FlagHasCreate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.row.Flags(); got != tt.expected {
				t.Errorf("Row.Flags() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestRowCodecV0DecodeErrors(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expectedErr string
	}{
		{
			name:        "Too short",
			input:       []byte{0, 1, 2},
			expectedErr: "RowCodecV0 corrupt: data length too short to decode a row",
		},
		{
			name:        "Invalid key suffix length",
			input:       []byte{0, 1, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: "RowCodecV0 corrupt: data length too short for key suffix",
		},
		{
			name:        "Invalid expire timestamp",
			input:       []byte{0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
			expectedErr: "RowCodecV0 corrupt: data length too short for expire",
		},
		{
			name:        "Invalid create timestamp",
			input:       []byte{0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4},
			expectedErr: "RowCodecV0 corrupt: data length too short for create",
		},
		{
			name:        "Invalid value length",
			input:       []byte{0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedErr: "RowCodecV0 corrupt: data length too short for for value length",
		},
		{
			name:        "Invalid value",
			input:       []byte{0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5},
			expectedErr: "RowCodecV0 corrupt: data length too short for for value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := block.RowCodecV0.Decode(tt.input)
			if err == nil || err.Error() != tt.expectedErr {
				t.Errorf("Expected error '%s', got '%v'", tt.expectedErr, err)
			}
		})
	}
}

func TestRowCodecV0EncodeAndDecode(t *testing.T) {
	tests := []struct {
		name           string
		row            block.Row
		firstKeyPrefix []byte
	}{
		{
			name: "NormalRowWithExpireAt",
			row: block.Row{
				KeyPrefixLen: 3,
				KeySuffix:    []byte("key"),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    nil,
				ExpireAt:     ptr(int64(10)),
			},
			firstKeyPrefix: []byte("prefixdata"),
		},
		{
			name: "NormalRowWithoutExpireAt",
			row: block.Row{
				KeyPrefixLen: 0,
				KeySuffix:    []byte("key"),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    nil,
				ExpireAt:     nil,
			},
			firstKeyPrefix: []byte(""),
		},
		{
			name: "RowWithBothTimestamps",
			row: block.Row{
				KeyPrefixLen: 5,
				KeySuffix:    []byte("both"),
				Seq:          100,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    ptr(int64(1234567890)),
				ExpireAt:     ptr(int64(9876543210)),
			},
			firstKeyPrefix: []byte("test_both"),
		},
		{
			name: "RowWithOnlyCreateAt",
			row: block.Row{
				KeyPrefixLen: 4,
				KeySuffix:    []byte("create"),
				Seq:          50,
				Value:        types.Value{Value: []byte("test_value")},
				CreatedAt:    ptr(int64(1234567890)),
				ExpireAt:     nil,
			},
			firstKeyPrefix: []byte("timecreate"),
		},
		{
			name: "TombstoneRow",
			row: block.Row{
				KeyPrefixLen: 4,
				KeySuffix:    []byte("tomb"),
				Seq:          1,
				Value:        types.Value{Kind: types.KindTombStone},
				CreatedAt:    ptr(int64(2)),
				ExpireAt:     ptr(int64(1)),
			},
			firstKeyPrefix: []byte("deadbeefdata"),
		},
		{
			name: "EmptyKeySuffix",
			row: block.Row{
				KeyPrefixLen: 4,
				KeySuffix:    []byte(""),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    nil,
				ExpireAt:     nil,
			},
			firstKeyPrefix: []byte("keyprefixdata"),
		},
		{
			name: "LargeSequenceNumber",
			row: block.Row{
				KeyPrefixLen: 3,
				KeySuffix:    []byte("seq"),
				Seq:          ^uint64(0),
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    nil,
				ExpireAt:     nil,
			},
			firstKeyPrefix: []byte("bigseq"),
		},
		{
			name: "LargeValue",
			row: block.Row{
				KeyPrefixLen: 2,
				KeySuffix:    []byte("big"),
				Seq:          1,
				Value:        types.Value{Value: bytes.Repeat([]byte("x"), 100)},
				CreatedAt:    nil,
				ExpireAt:     nil,
			},
			firstKeyPrefix: []byte("bigvalue"),
		},
		{
			name: "LongKeySuffix",
			row: block.Row{
				KeyPrefixLen: 2,
				KeySuffix:    bytes.Repeat([]byte("k"), 100),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    nil,
				ExpireAt:     nil,
			},
			firstKeyPrefix: []byte("longkey"),
		},
		{
			name: "UnicodeKeySuffix",
			row: block.Row{
				KeyPrefixLen: 3,
				KeySuffix:    []byte("你好世界"),
				Seq:          1,
				Value:        types.Value{Value: []byte("value")},
				CreatedAt:    nil,
				ExpireAt:     nil,
			},
			firstKeyPrefix: []byte("unicode"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the row
			encoded := block.RowCodecV0.Encode(tt.row)

			// Decode the row
			decoded, err := block.RowCodecV0.Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, &tt.row, decoded)

			// Test restoring full key
			fullKey := tt.row.FullKey(tt.firstKeyPrefix)
			assert.Equal(t, append(tt.firstKeyPrefix[:tt.row.KeyPrefixLen], decoded.KeySuffix...), fullKey)
			//t.Logf("FullKey: %s", fullKey)
		})
	}
}

func ptr(i int64) *int64 {
	return &i
}

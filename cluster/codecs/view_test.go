package codecs_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
)

func TestReadVarSlice(t *testing.T) {
	cases := []struct {
		name    string
		bufLen  int
		offset  int
		length  uint32
		payload string
		maxLen  int
		wantErr error
		wantLen int
	}{
		{
			name:    "OK",
			bufLen:  16,
			offset:  2,
			length:  4,
			payload: "data",
			maxLen:  8,
			wantLen: 4,
		},
		{
			name:    "ZeroLengthPayload",
			bufLen:  8,
			offset:  0,
			length:  0,
			maxLen:  -1,
			wantLen: 0,
		},
		{
			name:    "MaxLenZeroLengthZero",
			bufLen:  8,
			offset:  0,
			length:  0,
			maxLen:  0,
			wantLen: 0,
		},
		{
			name:    "MaxLenExactBoundary",
			bufLen:  12,
			offset:  0,
			length:  4,
			payload: "abcd",
			maxLen:  4,
			wantLen: 4,
		},
		{
			name:    "PayloadEndsAtBufferEnd",
			bufLen:  8,
			offset:  0,
			length:  4,
			payload: "abcd",
			maxLen:  -1,
			wantLen: 4,
		},
		{
			name:    "NegativeOffset",
			bufLen:  8,
			offset:  -1,
			maxLen:  -1,
			wantErr: codecs.ErrVarSliceOutOfBounds,
		},
		{
			name:    "OffsetMaxInt",
			bufLen:  8,
			offset:  math.MaxInt,
			maxLen:  -1,
			wantErr: codecs.ErrVarSliceOutOfBounds,
		},
		{
			name:    "PayloadOverflow",
			bufLen:  8,
			offset:  0,
			length:  8,
			maxLen:  -1,
			wantErr: codecs.ErrVarSliceOutOfBounds,
		},
		{
			name:    "LengthPrefixOverflow",
			bufLen:  2,
			offset:  0,
			maxLen:  -1,
			wantErr: codecs.ErrVarSliceOutOfBounds,
		},
		{
			name:    "OneByteShort",
			bufLen:  7,
			offset:  0,
			length:  4,
			payload: "abc",
			maxLen:  -1,
			wantErr: codecs.ErrVarSliceOutOfBounds,
		},
		{
			name:    "ExceedsMaxLen",
			bufLen:  16,
			offset:  0,
			length:  8,
			payload: "12345678",
			maxLen:  4,
			wantErr: codecs.ErrVarSliceTooLarge,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			if tc.offset >= 0 && tc.offset <= tc.bufLen && tc.bufLen-tc.offset >= 4 {
				binary.LittleEndian.PutUint32(buf[tc.offset:], tc.length)
				if tc.payload != "" {
					copy(buf[tc.offset+4:], []byte(tc.payload))
				}
			}

			got, next, err := codecs.ReadVarSlice(buf, tc.offset, tc.maxLen)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Len(t, got.Data, tc.wantLen)
			if tc.payload != "" {
				require.Equal(t, []byte(tc.payload), got.Data)
			}
			require.Equal(t, tc.offset+4+tc.wantLen, next)
		})
	}
}

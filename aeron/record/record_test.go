package record_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/record"
)

func TestRecord(t *testing.T) {
	const recordOffset = 16

	t.Run("constants", func(t *testing.T) {
		cases := []struct {
			name string
			got  int
			want int
		}{
			{"HeaderLength", record.HeaderLength, 8},
			{"Alignment", record.Alignment, record.HeaderLength},
			{"TypeFieldOffset", record.TypeFieldOffset, 4},
			{"PaddingMsgTypeID", record.PaddingMsgTypeID, -1},
			{"InsufficientCapacity", record.InsufficientCapacity, -2},
		}
		for _, tc := range cases {
			require.Equal(t, tc.want, tc.got, tc.name)
		}
	})

	t.Run("offsets", func(t *testing.T) {
		cases := []struct {
			name string
			got  int
			want int
		}{
			{"LengthOffset", record.LengthOffset(recordOffset), recordOffset},
			{"TypeOffset", record.TypeOffset(recordOffset), recordOffset + record.TypeFieldOffset},
			{"EncodedMsgOffset", record.EncodedMsgOffset(recordOffset), recordOffset + record.HeaderLength},
		}
		for _, tc := range cases {
			require.Equal(t, tc.want, tc.got, tc.name)
		}
	})

	t.Run("AlignRecordLength", func(t *testing.T) {
		require.Equal(t, 16, record.AlignRecordLength(9))
	})

	t.Run("CheckTypeID", func(t *testing.T) {
		require.NoError(t, record.CheckTypeID(1))
		for _, id := range []int{0, -1} {
			err := record.CheckTypeID(id)
			require.ErrorIs(t, err, record.ErrInvalidMessageTypeID)
		}
	})
}

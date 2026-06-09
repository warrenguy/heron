package logbuffer_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestComputePosition_table(t *testing.T) {
	const (
		initialTermID = int32(7)
		shift         = 16
	)
	cases := []struct {
		name         string
		activeTermID int32
		termOffset   int32
		wantPosition int64
	}{
		{"term7 offset0", 7, 0, 0},
		{"term8 offset100", 8, 100, 65636},
		{"term9 offset0", 9, 0, 131072},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := logbuffer.ComputePosition(tc.activeTermID, tc.termOffset, shift, initialTermID)
			require.Equal(t, tc.wantPosition, got)
		})
	}
}

func TestIndexByPosition_table(t *testing.T) {
	const shift = 16
	cases := []struct {
		name      string
		position  int64
		wantIndex int
	}{
		{"partition0", 0, 0},
		{"partition1", 65636, 1},
		{"partition2", 131072, 2},
		{"partition0 wrap", 196608, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantIndex, logbuffer.IndexByPosition(tc.position, shift))
		})
	}
}

func TestIndexByTermCount_unsignedModulo(t *testing.T) {
	cases := []struct {
		name      string
		termCount int32
		want      int
	}{
		{"zero", 0, 0},
		{"one", 1, 1},
		{"negative one", -1, 0},
		{"min int32", math.MinInt32, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, logbuffer.IndexByTermCount(tc.termCount))
		})
	}
}

func TestIndexByPosition_negativePosition(t *testing.T) {
	const shift = 16
	require.Equal(t, 0, logbuffer.IndexByPosition(-1, shift))
}

func TestWrapArithmetic(t *testing.T) {
	cases := []struct {
		name string
		fn   func() int32
		want int32
	}{
		{"add max int32 and 1", func() int32 {
			return logbuffer.AddWrapI32(math.MaxInt32, 1)
		}, math.MinInt32},
		{"sub 0 and 1", func() int32 {
			return logbuffer.SubWrapI32(0, 1)
		}, -1},
		{"sub equal", func() int32 {
			return logbuffer.SubWrapI32(7, 7)
		}, 0},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, tc.fn(), tc.name)
	}
}

func TestComputeTermBeginPosition(t *testing.T) {
	const (
		initialTermID = int32(7)
		shift         = 16
	)
	require.Equal(t, int64(0), logbuffer.ComputeTermBeginPosition(7, shift, initialTermID))
	require.Equal(t, int64(65536), logbuffer.ComputeTermBeginPosition(8, shift, initialTermID))
}

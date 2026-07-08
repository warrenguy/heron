package atomicbuffer_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
)

func TestIsPowerOfTwo(t *testing.T) {
	tests := []struct {
		value int
		want  bool
	}{
		{1024, true},
		{1023, false},
	}
	for _, test := range tests {
		require.Equal(t, test.want, atomicbuffer.IsPowerOfTwo(test.value))
	}
}

func TestNumberOfTrailingZeroes(t *testing.T) {
	tests := []struct {
		value int32
		want  int
	}{
		{0, 32},
		{8, 3},
	}
	for _, test := range tests {
		require.Equal(t, test.want, atomicbuffer.NumberOfTrailingZeroes(test.value))
	}
}

func TestFindNextPowerOfTwo(t *testing.T) {
	tests := []struct {
		value int32
		want  int32
	}{
		{0, 1},
		{-1, 1},
		{1000, 1024},
		{1, 1},
		{2, 2},
		{1073741823, 1073741824},
		{1073741824, 1073741824},
		{1073741825, math.MinInt32},
		{2147483647, math.MinInt32},
	}
	for _, test := range tests {
		require.Equal(t, test.want, atomicbuffer.FindNextPowerOfTwo(test.value))
	}
}

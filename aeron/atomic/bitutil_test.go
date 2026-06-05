package atomic_test

import (
	"math"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
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
		if got := atomic.IsPowerOfTwo(test.value); got != test.want {
			t.Fatalf("IsPowerOfTwo(%d)=%v want %v", test.value, got, test.want)
		}
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
		if got := atomic.NumberOfTrailingZeroes(test.value); got != test.want {
			t.Fatalf("NumberOfTrailingZeroes(%d)=%d want %d", test.value, got, test.want)
		}
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
		if got := atomic.FindNextPowerOfTwo(test.value); got != test.want {
			t.Fatalf("FindNextPowerOfTwo(%d)=%d want %d", test.value, got, test.want)
		}
	}
}

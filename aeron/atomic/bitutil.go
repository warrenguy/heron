package atomic

import (
	"math"
	"math/bits"
)

const CacheLineLength = 64

const maxPowerOfTwoInt32 = 1 << 30

// Align rounds value up to alignment. alignment must be a positive power of two.
func Align(value, alignment int) int {
	return (value + alignment - 1) &^ (alignment - 1)
}

func IsPowerOfTwo(value int) bool {
	return value > 0 && bits.OnesCount(uint(value)) == 1
}

func NumberOfTrailingZeroes(value int32) int {
	return bits.TrailingZeros32(uint32(value))
}

// FindNextPowerOfTwo returns the next power of two at or above value.
// For value <= 0 returns 1. For value > 2^30 returns math.MinInt32 (Agrona overflow sentinel).
func FindNextPowerOfTwo(value int32) int32 {
	if value <= 0 {
		return 1
	}
	if value > maxPowerOfTwoInt32 {
		return math.MinInt32
	}
	return int32(1 << bits.Len32(uint32(value-1)))
}

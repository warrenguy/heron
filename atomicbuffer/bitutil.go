package atomicbuffer

import "math/bits"

const CacheLineLength = 64

const maxPowerOfTwoInt32 = 1 << 30

// findNextPowerOfTwoOverflow is Agrona's Integer.MIN_VALUE sentinel for out-of-range input.
const findNextPowerOfTwoOverflow int32 = -1 << 31

// Align rounds value up to alignment. alignment must be a positive power of two.
func Align(value, alignment int) int {
	if alignment <= 0 || !IsPowerOfTwo(alignment) {
		panic("atomicbuffer: alignment must be a positive power of two")
	}
	return (value + alignment - 1) & -alignment
}

func IsPowerOfTwo(value int) bool {
	return value > 0 && value&(value-1) == 0
}

func NumberOfTrailingZeroes(value int32) int {
	//nolint:gosec // G115: counts trailing zeros of the bit pattern
	return bits.TrailingZeros32(uint32(value))
}

// FindNextPowerOfTwo returns the next power of two at or above value.
// For value <= 0 returns 1. For value > 2^30 returns findNextPowerOfTwoOverflow.
func FindNextPowerOfTwo(value int32) int32 {
	if value <= 0 {
		return 1
	}
	if value > maxPowerOfTwoInt32 {
		return findNextPowerOfTwoOverflow
	}
	return int32(1 << bits.Len32(uint32(value-1)))
}

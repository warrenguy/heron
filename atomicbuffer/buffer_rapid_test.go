//go:build rapid

package atomicbuffer_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/atomicbuffer"
)

func drawOutOfBoundsOffset(rt *rapid.T, capacity int) int {
	rt.Helper()
	return rapid.IntRange(-64, capacity+64).
		Filter(func(offset int) bool { return offset < 0 || offset >= capacity }).
		Draw(rt, "offset")
}

func TestGetByte_outOfBounds_panics_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(0, 128).Draw(rt, "capacity")
		offset := drawOutOfBoundsOffset(rt, capacity)
		buf := atomicbuffer.Wrap(make([]byte, capacity))
		require.Panics(rt, func() { buf.GetByte(offset) })
	})
}

func TestPutByte_outOfBounds_panics_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(0, 128).Draw(rt, "capacity")
		offset := drawOutOfBoundsOffset(rt, capacity)
		value := rapid.Byte().Draw(rt, "value")
		buf := atomicbuffer.Wrap(make([]byte, capacity))
		require.Panics(rt, func() { buf.PutByte(offset, value) })
	})
}

func TestGetVolatile_bounds_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		words := rapid.IntRange(0, 16).Draw(rt, "words")
		capacity := words * atomicbuffer.SizeInt64
		buffer := atomicbuffer.Wrap(make([]byte, capacity))
		offset := rapid.OneOf(
			rapid.Map(rapid.IntRange(-8, words+8), func(word int) int {
				return word * atomicbuffer.SizeInt64
			}),
			rapid.SampledFrom([]int{
				math.MaxInt - 7,
				math.MaxInt - 15,
				math.MinInt,
				math.MinInt + 8,
			}),
		).Draw(rt, "offset")

		accesses := []struct {
			size int
			read func()
		}{
			{atomicbuffer.SizeInt32, func() { _ = buffer.GetInt32Volatile(offset) }},
			{atomicbuffer.SizeInt64, func() { _ = buffer.GetInt64Volatile(offset) }},
		}

		for _, access := range accesses {
			inBounds := offset >= 0 && offset <= capacity-access.size
			if inBounds {
				require.NotPanics(rt, access.read)
				continue
			}

			require.Panics(rt, access.read)
		}
	})
}

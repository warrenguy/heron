//go:build rapid

package logbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestTermMath_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		shift := rapid.IntRange(16, 20).Draw(t, "shift")
		termLength := 1 << shift
		initialTermID := rapid.Int32().Draw(t, "init")
		termCount := rapid.Int32Range(0, 1<<12).Draw(t, "termCount")
		termOffset := rapid.Int32Range(0, int32(termLength-1)).Draw(t, "off")
		positionBitsToShift := logbuffer.PositionBitsToShift(termLength)
		activeTermID := logbuffer.AddWrapI32(initialTermID, termCount)

		position := logbuffer.ComputePosition(
			activeTermID,
			termOffset,
			positionBitsToShift,
			initialTermID,
		)
		require.Equal(
			t,
			activeTermID,
			logbuffer.ComputeTermIDFromPosition(position, positionBitsToShift, initialTermID),
		)
		require.Equal(
			t,
			termOffset,
			logbuffer.ComputeTermOffsetFromPosition(position, positionBitsToShift),
		)
		require.Equal(
			t,
			logbuffer.IndexByTerm(initialTermID, activeTermID),
			logbuffer.IndexByPosition(position, positionBitsToShift),
		)
	})
}

func TestIndexByTermCount_unsignedModulo_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		termCount := rapid.Int32().Draw(t, "termCount")
		want := int(uint32(termCount) % uint32(logbuffer.PartitionCount))
		require.Equal(t, want, logbuffer.IndexByTermCount(termCount))
	})
}

func TestWrapArithmetic_modular_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		a := rapid.Int32().Draw(t, "a")
		b := rapid.Int32().Draw(t, "b")
		init := rapid.Int32().Draw(t, "init")

		sum := logbuffer.AddWrapI32(a, b)
		require.Equal(
			t,
			logbuffer.ComputeTermCount(sum, init),
			logbuffer.AddWrapI32(logbuffer.ComputeTermCount(a, init), b),
		)
	})
}

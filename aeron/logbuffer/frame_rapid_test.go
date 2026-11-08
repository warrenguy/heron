//go:build rapid

package logbuffer_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"luas.fast/aeron/logbuffer"
	"luas.fast/atomicbuffer"
)

func TestCommittedFrameLengthVolatile_termEndSlack_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		paddingLength := rapid.Int32Range(1, 31).Draw(t, "paddingLength")

		var wantLength int32
		if paddingLength >= 4 {
			wantLength = paddingLength
		}

		buf := make([]byte, paddingLength)
		buffer := atomicbuffer.Wrap(buf)
		logbuffer.CommitMinimalTermEndPadding(buffer, 0, paddingLength)

		require.Zero(t, logbuffer.FrameLengthVolatile(buffer, 0))
		require.Equal(t, wantLength, logbuffer.CommittedFrameLengthVolatile(buffer, 0))
	})
}

func TestFrameRead_offsetOutOfBounds_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(0, 128).Draw(rt, "capacity")
		buffer := atomicbuffer.Wrap(make([]byte, capacity))
		offset := rapid.OneOf(
			rapid.IntRange(-256, -1),
			rapid.IntRange(capacity, capacity+64),
			rapid.SampledFrom([]int{
				math.MaxInt,
				math.MaxInt - 7,
				math.MinInt,
				math.MinInt + 8,
			}),
		).Draw(rt, "offset")

		require.Zero(rt, logbuffer.FrameLength(buffer, offset))
		require.Zero(rt, logbuffer.FrameLengthVolatile(buffer, offset))
		require.Zero(rt, logbuffer.CommittedFrameLengthVolatile(buffer, offset))
		require.Zero(rt, logbuffer.FrameReservedValue(buffer, offset))
		require.False(rt, logbuffer.IsPaddingFrame(buffer, offset))
	})
}

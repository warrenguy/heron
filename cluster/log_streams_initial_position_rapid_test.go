//go:build rapid

package cluster

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

// TestLogChannelWithInitialPosition_geometry_rapid checks that seeded URI params
// match logbuffer position geometry for random valid positions.
func TestLogChannelWithInitialPosition_geometry_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		shift := rapid.IntRange(16, 20).Draw(rt, "shift")
		termLength := 1 << shift
		initialTermID := rapid.Int32().Draw(rt, "initTermID")

		// MTU must be frame-aligned and within the UDP payload ceiling.
		maxMtuFrames := logbuffer.MaxUDPPayloadLength / logbuffer.FrameAlignment
		mtuFrames := rapid.IntRange(2, maxMtuFrames).Draw(rt, "mtuFrames")
		mtu := int32(mtuFrames * logbuffer.FrameAlignment)

		// Non-zero stream position: at least one frame into some term.
		termCount := rapid.Int32Range(0, 1<<10).Draw(rt, "termCount")
		maxFrame := termLength / logbuffer.FrameAlignment
		minFrame := 0
		if termCount == 0 {
			minFrame = 1
		}
		frameSlot := rapid.IntRange(minFrame, maxFrame-1).Draw(rt, "frameSlot")
		termOffset := int32(frameSlot * logbuffer.FrameAlignment)
		activeTermID := logbuffer.AddWrapI32(initialTermID, termCount)
		position := logbuffer.ComputePosition(
			activeTermID,
			int(termOffset),
			shift,
			initialTermID,
		)
		require.Positive(rt, position)
		require.Zero(rt, position&(logbuffer.FrameAlignment-1))

		const base = "aeron:udp?term-length=64k|endpoint=239.255.12.1:9004"
		got, err := logChannelWithInitialPosition(base, &LogPublicationInitialPosition{
			Position:         position,
			InitialTermID:    initialTermID,
			TermBufferLength: int32(termLength),
			MtuLength:        mtu,
		})
		require.NoError(rt, err)

		wantTermID := logbuffer.ComputeTermIDFromPosition(position, shift, initialTermID)
		wantOffset := logbuffer.ComputeTermOffsetFromPosition(position, shift)
		require.Equal(rt, wantTermID, activeTermID)
		require.Equal(rt, wantOffset, termOffset)

		require.Equal(
			rt,
			strconv.FormatInt(int64(initialTermID), 10),
			channelParamValue(got, channelInitTermIDParam),
		)
		require.Equal(
			rt,
			strconv.FormatInt(int64(wantTermID), 10),
			channelParamValue(got, channelTermIDParam),
		)
		require.Equal(
			rt,
			strconv.FormatInt(int64(wantOffset), 10),
			channelParamValue(got, channelTermOffsetParam),
		)
		require.Equal(rt, strconv.Itoa(termLength), channelParamValue(got, channelTermLengthParam))
		require.Equal(
			rt,
			strconv.FormatInt(int64(mtu), 10),
			channelParamValue(got, channelMTUParam),
		)
	})
}

// TestLogChannelWithInitialPosition_zeroPosition_rapid checks position zero still
// seeds init-term geometry when a seed is present.
func TestLogChannelWithInitialPosition_zeroPosition_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		shift := rapid.IntRange(16, 20).Draw(rt, "shift")
		termLength := 1 << shift
		initialTermID := rapid.Int32().Draw(rt, "initTermID")
		mtuFrames := rapid.IntRange(2, 64).Draw(rt, "mtuFrames")
		mtu := int32(mtuFrames * logbuffer.FrameAlignment)

		const base = "aeron:udp?endpoint=239.255.12.1:9004"
		got, err := logChannelWithInitialPosition(base, &LogPublicationInitialPosition{
			Position:         0,
			InitialTermID:    initialTermID,
			TermBufferLength: int32(termLength),
			MtuLength:        mtu,
		})
		require.NoError(rt, err)
		initTerm := strconv.FormatInt(int64(initialTermID), 10)
		require.Equal(rt, initTerm, channelParamValue(got, channelInitTermIDParam))
		require.Equal(rt, initTerm, channelParamValue(got, channelTermIDParam))
		require.Equal(rt, "0", channelParamValue(got, channelTermOffsetParam))
		require.Equal(rt, strconv.Itoa(termLength), channelParamValue(got, channelTermLengthParam))
		require.Equal(
			rt,
			strconv.FormatInt(int64(mtu), 10),
			channelParamValue(got, channelMTUParam),
		)
	})
}

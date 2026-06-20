//go:build rapid

package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

func writePollTestRandomFrames(
	rt *rapid.T,
	termBuffer atomic.AtomicBuffer,
	namePrefix string,
	frameCount int,
	maxPayloadLen int,
	flags uint8,
) []byte {
	offset := 0
	expectedFlags := make([]byte, 0, frameCount)
	for range frameCount {
		payloadLen := rapid.IntRange(0, maxPayloadLen).Draw(rt, namePrefix+"payloadLen")
		frameLength := logbuffer.AlignFrameLength(payloadLen + logbuffer.DataHeaderLength)
		if offset+frameLength > termBuffer.Capacity() {
			break
		}
		payload := make([]byte, payloadLen)
		writePollTestDataFrame(termBuffer, offset, int32(offset), flags, payload)
		expectedFlags = append(expectedFlags, byte(flags))
		offset += logbuffer.AlignFragmentChunkLength(len(payload))
	}
	return expectedFlags
}

func TestImage_joinAndLivePosition_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		joinPosition := rapid.Int64().Draw(rt, "joinPosition")

		sub := testSubscription(t)
		image := aeron.NewImageForTest(t, sub, joinPosition)

		require.Equal(rt, joinPosition, image.JoinPosition())
		require.Equal(rt, joinPosition, image.Position())

		livePosition := rapid.Int64().Draw(rt, "livePosition")
		aeron.ImageSetSubscriberPositionForTest(image, livePosition)

		require.Equal(rt, joinPosition, image.JoinPosition())
		require.Equal(rt, livePosition, image.Position())

		aeron.ImageInternalCloseForTest(image)

		postClosePosition := rapid.Int64().Draw(rt, "postClosePosition")
		aeron.ImageSetSubscriberPositionForTest(image, postClosePosition)

		require.Equal(rt, livePosition, image.Position())
		require.Equal(rt, joinPosition, image.JoinPosition())
	})
}

func TestImage_Poll_positionMonotonic_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)

		entryCount := rapid.IntRange(1, 12).Draw(rt, "entryCount")
		offset := 0
		for range entryCount {
			if offset+logbuffer.DataHeaderLength >= termBuffer.Capacity() {
				break
			}
			if rapid.Bool().Draw(rt, "isPadding") {
				paddingUnits := rapid.IntRange(1, 3).Draw(rt, "paddingUnits")
				paddingLength := int32(paddingUnits * logbuffer.FrameAlignment)
				if offset+int(paddingLength) > termBuffer.Capacity() {
					break
				}
				aeron.WriteCommittedPaddingFrameForTest(
					termBuffer,
					offset,
					aeron.TestPublicationInitialTermID,
					int32(offset),
					paddingLength,
				)
				offset += int(paddingLength)
				continue
			}

			payloadLen := rapid.IntRange(0, 32).Draw(rt, "payloadLen")
			frameLength := logbuffer.AlignFrameLength(payloadLen + logbuffer.DataHeaderLength)
			if offset+frameLength > termBuffer.Capacity() {
				break
			}
			payload := make([]byte, payloadLen)
			writePollTestDataFrame(termBuffer, offset, int32(offset), logbuffer.Unfragmented, payload)
			offset += logbuffer.AlignFragmentChunkLength(len(payload))
		}

		fragmentLimit := rapid.IntRange(1, entryCount).Draw(rt, "fragmentLimit")
		positionBefore := image.Position()
		fragments := image.Poll(capture.handler, fragmentLimit)
		positionAfter := image.Position()

		require.GreaterOrEqual(rt, positionAfter, positionBefore)
		if offset > 0 {
			require.Greater(rt, positionAfter, positionBefore)
		}
		require.LessOrEqual(rt, fragments, fragmentLimit)
	})
}

func TestImage_Poll_paddingSkip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)

		paddingLength := int32(rapid.IntRange(1, 4).Draw(rt, "paddingUnits") * logbuffer.FrameAlignment)
		payload := []byte{byte(rapid.Byte().Draw(rt, "payloadByte"))}
		writePollTestPaddingThenData(termBuffer, paddingLength, payload)

		fragments := image.Poll(capture.handler, 2)

		require.Equal(rt, 1, fragments)
		require.Equal(rt, payload, capture.payloads[0])
		require.Equal(
			rt,
			int64(paddingLength)+int64(logbuffer.AlignFragmentChunkLength(len(payload))),
			image.Position(),
		)
	})
}

func TestImage_Poll_fragmentBoundaries_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)

		frameCount := rapid.IntRange(1, 8).Draw(rt, "frameCount")
		flags := uint8(logbuffer.BeginFlag | logbuffer.EndFlag)
		expectedFlags := writePollTestRandomFrames(
			rt,
			termBuffer,
			"",
			frameCount,
			int(aeron.TestPublicationMTULength)-logbuffer.DataHeaderLength,
			flags,
		)

		fragments := image.Poll(capture.handler, frameCount)

		require.Equal(rt, len(expectedFlags), fragments)
		require.Len(rt, capture.payloads, len(expectedFlags))
		require.Len(rt, capture.flags, len(expectedFlags))
		for i, gotFlags := range expectedFlags {
			require.Equal(rt, gotFlags, capture.flags[i])
		}
	})
}

func TestImage_Poll_alignRoundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)

		frameCount := rapid.IntRange(1, 6).Draw(rt, "frameCount")
		offset := 0
		for range frameCount {
			payloadLen := rapid.IntRange(0, 48).Draw(rt, "payloadLen")
			frameLength := logbuffer.AlignFrameLength(payloadLen + logbuffer.DataHeaderLength)
			if offset+frameLength > termBuffer.Capacity() {
				break
			}
			payload := make([]byte, payloadLen)
			writePollTestDataFrame(termBuffer, offset, int32(offset), logbuffer.Unfragmented, payload)
			require.Zero(rt, offset%logbuffer.FrameAlignment)
			offset += logbuffer.AlignFragmentChunkLength(len(payload))
			require.Zero(rt, offset%logbuffer.FrameAlignment)
		}

		_ = image.Poll(capture.handler, frameCount)
		require.Equal(rt, int64(offset), image.Position())
	})
}

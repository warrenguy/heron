package aeron_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestPublication_Offer_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		pub := aeron.NewTestPublicationWithoutTBForRapid(
			aeron.DefaultTestPublicationLogConfig(),
			int64(1)<<40,
		)
		maxPayload := pub.MaxPayloadLengthForTest()
		messageCount := rapid.IntRange(1, 32).Draw(t, "messageCount")
		shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)

		var lastPosition int64
		for i := range messageCount {
			length := rapid.IntRange(0, maxPayload).Draw(t, fmt.Sprintf("length%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("payload%d", i))

			position := aeron.OfferRetryForTest(pub, payload)
			require.Positive(t, position)
			if i > 0 {
				require.Greater(t, position, lastPosition)
			}
			lastPosition = position
			aligned := logbuffer.AlignFrameLength(length + logbuffer.DataHeaderLength)
			frameStart := int(logbuffer.ComputeTermOffsetFromPosition(position, shift)) - aligned

			termIndex := logbuffer.IndexByPosition(position, shift)
			termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameStart))
			require.Equal(t, length+logbuffer.DataHeaderLength, frameLength)
			got := termBuf.Bytes()[frameStart+logbuffer.DataHeaderLength : frameStart+frameLength]
			require.Equal(t, payload, got)
		}
	})
}

func TestPublication_Offer_rapid_nonZeroStart(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
		maxStart := int64(aeron.TestPublicationTermLength / 4)
		startPosition := rapid.Int64Range(1, maxStart).Draw(t, "startPosition")
		cfg := publicationLogConfigAtPosition(startPosition)
		pub := aeron.NewTestPublicationWithoutTBForRapid(cfg, int64(1)<<40)

		maxPayload := pub.MaxPayloadLengthForTest()
		messageCount := rapid.IntRange(1, 8).Draw(t, "messageCount")

		var lastPosition int64
		for i := range messageCount {
			maxLength := min(maxPayload, 64)
			length := rapid.IntRange(0, maxLength).Draw(t, fmt.Sprintf("length%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("payload%d", i))

			position := aeron.OfferRetryForTest(pub, payload)
			require.Positive(t, position)
			if i > 0 {
				require.Greater(t, position, lastPosition)
			}
			lastPosition = position

			aligned := logbuffer.AlignFrameLength(length + logbuffer.DataHeaderLength)
			frameStart := int(logbuffer.ComputeTermOffsetFromPosition(position, shift)) - aligned

			termIndex := logbuffer.IndexByPosition(position, shift)
			termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameStart))
			require.Equal(t, length+logbuffer.DataHeaderLength, frameLength)
			payloadStart := frameStart + logbuffer.DataHeaderLength
			got := termBuf.Bytes()[payloadStart : payloadStart+length]
			require.Equal(t, payload, got)
		}
	})
}

func TestPublication_Offer_rapid_nearMaxPosition(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		position := publicationPositionNearMaxPosition()
		cfg := publicationLogConfigNearMaxPosition()
		pub := aeron.NewTestPublicationWithoutTBForRapid(cfg, position)
		beforeCount := aeron.PublicationActiveTermCountForTest(pub)

		shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
		termOffset := int(logbuffer.ComputeTermOffsetFromPosition(position, shift))
		termIndex := logbuffer.IndexByPosition(position, shift)
		termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))

		messageCount := rapid.IntRange(1, 8).Draw(t, "messageCount")
		for i := range messageCount {
			length := rapid.IntRange(1, 64).Draw(t, fmt.Sprintf("length%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("payload%d", i))

			got := pub.Offer(payload)
			require.Equal(t, aeron.MaxPositionExceeded, got)
		}

		require.Equal(t, beforeCount, aeron.PublicationActiveTermCountForTest(pub))
		require.Equal(t, position, pub.Position())
		require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))
	})
}

func TestPublication_TryClaim_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		pub := aeron.NewTestPublicationWithoutTBForRapid(
			aeron.DefaultTestPublicationLogConfig(),
			int64(1)<<40,
		)
		maxPayload := pub.MaxPayloadLengthForTest()
		messageCount := rapid.IntRange(1, 16).Draw(t, "messageCount")

		for i := range messageCount {
			length := rapid.IntRange(1, maxPayload).Draw(t, fmt.Sprintf("length%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("payload%d", i))
			abort := rapid.Bool().Draw(t, fmt.Sprintf("abort%d", i))

			termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(pub, length)
			require.NoError(t, err)
			require.Positive(t, position)

			termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
			if abort {
				require.NoError(t, pub.Abort(termIndex, frameOffset))
				require.True(t, logbuffer.IsPaddingFrame(termBuf, frameOffset))
				continue
			}

			termBuf.PutBytes(frameOffset+logbuffer.DataHeaderLength, payload)
			require.NoError(t, pub.Commit(termIndex, frameOffset))

			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameOffset))
			got := termBuf.Bytes()[frameOffset+logbuffer.DataHeaderLength : frameOffset+frameLength]
			require.Equal(t, payload, got)
		}
	})
}

//go:build rapid

package aeron_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestPublication_Offer_rapid(t *testing.T) {
	t.Parallel()
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
			aligned := logbuffer.AlignFragmentChunkLength(length)
			frameStart := aeron.FragmentedMessageStartOffsetForTest(position, shift, aligned)

			termIndex := logbuffer.IndexByPosition(position, shift)
			termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameStart))
			require.Equal(t, length+logbuffer.DataHeaderLength, frameLength)
			got := termBuf.Bytes()[frameStart+logbuffer.OffsetFramePayload : frameStart+frameLength]
			require.Equal(t, payload, got)
		}
	})
}

func TestPublication_Offer_rapid_nonZeroStart(t *testing.T) {
	t.Parallel()
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

			aligned := logbuffer.AlignFragmentChunkLength(length)
			frameStart := aeron.FragmentedMessageStartOffsetForTest(position, shift, aligned)

			termIndex := logbuffer.IndexByPosition(position, shift)
			termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameStart))
			require.Equal(t, length+logbuffer.DataHeaderLength, frameLength)
			payloadStart := frameStart + logbuffer.OffsetFramePayload
			got := termBuf.Bytes()[payloadStart : payloadStart+length]
			require.Equal(t, payload, got)
		}
	})
}

func TestPublication_Offer_rapid_nearMaxPosition_fragmented(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		position := publicationPositionNearMaxPosition()
		cfg := publicationLogConfigNearMaxPosition()
		pub := aeron.NewTestPublicationWithoutTBForRapid(cfg, position)
		beforeCount := aeron.PublicationActiveTermCountForTest(pub)
		maxPayload := pub.MaxPayloadLengthForTest()
		maxMessage := pub.MaxMessageLengthForTest()

		shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
		termOffset := int(logbuffer.ComputeTermOffsetFromPosition(position, shift))
		termIndex := logbuffer.IndexByPosition(position, shift)
		termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))

		messageCount := rapid.IntRange(1, 8).Draw(t, "messageCount")
		for i := range messageCount {
			length := rapid.IntRange(maxPayload+1, maxMessage).Draw(t, fmt.Sprintf("length%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("payload%d", i))

			got := pub.Offer(payload)
			require.Equal(t, aeron.MaxPositionExceeded, got)
		}

		require.Equal(t, beforeCount, aeron.PublicationActiveTermCountForTest(pub))
		require.Equal(t, position, pub.Position())
		require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termOffset))
	})
}

func TestPublication_Offer_rapid_nearMaxPosition(t *testing.T) {
	t.Parallel()
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

func TestPublication_Offer_rapid_fragmented(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		pub := aeron.NewTestPublicationWithoutTBForRapid(
			aeron.DefaultTestPublicationLogConfig(),
			int64(1)<<40,
		)
		maxPayload := pub.MaxPayloadLengthForTest()
		maxMessage := pub.MaxMessageLengthForTest()
		messageCount := rapid.IntRange(1, 8).Draw(t, "messageCount")

		var lastPosition int64
		type termReadCursor struct {
			termCount int32
			offset    int
		}
		termReadCursors := [logbuffer.PartitionCount]termReadCursor{}
		for i := range messageCount {
			length := rapid.IntRange(maxPayload+1, maxMessage).Draw(t, fmt.Sprintf("length%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("payload%d", i))

			position := aeron.OfferRetryForTest(pub, payload)
			require.Positive(t, position)
			if i > 0 {
				require.Greater(t, position, lastPosition)
			}
			lastPosition = position

			termCount := logbuffer.ActiveTermCount(pub.MetaDataBufferForTest())
			termIndex := logbuffer.IndexByTermCount(termCount)
			cursor := &termReadCursors[termIndex]
			if cursor.termCount != termCount {
				cursor.termCount = termCount
				cursor.offset = 0
			}

			termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
			frames, ok := aeron.ReadFragmentedMessageFramesForTest(termBuf, cursor.offset)
			require.True(t, ok)
			require.Equal(t, payload, reassembledPayload(frames))
			requireFragmentFlags(t, frames)
			cursor.offset += logbuffer.ComputeFragmentedFrameLength(length, maxPayload)
		}
	})
}

const limitGateRapidMaxDraw = 8192 // default 64 KiB term; avoid AdminAction rotation spin

func TestPublication_Offer_limitGate_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		limit := rapid.Int64Range(256, limitGateRapidMaxDraw).Draw(t, "limit")
		startPosition := rapid.Int64Range(0, limit).Draw(t, "startPosition")

		cfg := publicationLogConfigAtPosition(startPosition)
		if startPosition >= limit {
			connected := rapid.Bool().Draw(t, "connected")
			if connected {
				cfg.IsConnected = 1
			} else {
				cfg.IsConnected = 0
			}
			pub := aeron.NewTestPublicationWithoutTBForRapid(cfg, limit)

			maxPayload := pub.MaxPayloadLengthForTest()
			maxMessage := pub.MaxMessageLengthForTest()
			payloadLen := rapid.IntRange(maxPayload+1, maxMessage).Draw(t, "payloadLen")
			payload := rapid.SliceOfN(rapid.Byte(), payloadLen, payloadLen).Draw(t, "payload")

			got := pub.Offer(payload)
			if connected {
				require.Equal(t, aeron.BackPressured, got)
			} else {
				require.Equal(t, aeron.NotConnected, got)
			}
			return
		}

		connected := rapid.Bool().Draw(t, "connected")
		if connected {
			cfg.IsConnected = 1
		} else {
			cfg.IsConnected = 0
		}
		pub := aeron.NewTestPublicationWithoutTBForRapid(cfg, limit)
		maxPayload := pub.MaxPayloadLengthForTest()
		payloadLen := rapid.IntRange(0, maxPayload).Draw(t, "payloadLen")
		payload := rapid.SliceOfN(rapid.Byte(), payloadLen, payloadLen).Draw(t, "payload")
		require.Positive(t, aeron.OfferRetryForTest(pub, payload))
	})
}

// drawLengthRuleLogConfig draws an MTU and a publication state. Returns the
// log config and the publication limit.
func drawLengthRuleLogConfig(
	t *rapid.T,
	base func() aeron.TestPublicationLogConfig,
) (aeron.TestPublicationLogConfig, int64) {
	t.Helper()

	var cfg aeron.TestPublicationLogConfig
	position := aeron.PositionNearMaxPositionForTest()
	if rapid.Bool().Draw(t, "nearMaxPosition") {
		cfg = aeron.LogConfigNearMaxPositionForTest(base)
	} else {
		frameCount := rapid.Int64Range(0, 4*aeron.TestPublicationTermLength/logbuffer.FrameAlignment).
			Draw(t, "frameCount")
		position = frameCount * logbuffer.FrameAlignment
		cfg = aeron.LogConfigAtPositionForTest(base, position)
	}
	limit := int64(1) << 62
	if rapid.Bool().Draw(t, "atLimit") {
		limit = position
	}
	if rapid.Bool().Draw(t, "disconnected") {
		cfg.IsConnected = 0
	}
	cfg.MTULength = rapid.SampledFrom(
		[]int32{aeron.TestPublicationMTULength, mtuAboveMaxMessageLength},
	).Draw(t, "mtuLength")
	return cfg, limit
}

// drawOfferLength draws a length in [0, 2*maxMessage] or within one byte of
// either limit. Reports whether Offer must return UnsupportedMessageLength.
func drawOfferLength(t *rapid.T, maxPayload, maxMessage int) (length int, oversized bool) {
	t.Helper()

	nearLimits := []int{
		maxPayload - 1, maxPayload, maxPayload + 1,
		maxMessage - 1, maxMessage, maxMessage + 1,
	}
	length = rapid.OneOf(rapid.IntRange(0, 2*maxMessage), rapid.SampledFrom(nearLimits)).
		Draw(t, "length")
	return length, length > max(maxPayload, maxMessage)
}

func TestPublication_Offer_lengthRule_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg, limit := drawLengthRuleLogConfig(t, aeron.DefaultTestPublicationLogConfig)
		pub := aeron.NewTestPublicationWithoutTBForRapid(cfg, limit)

		length, oversized := drawOfferLength(t, pub.MaxPayloadLength(), pub.MaxMessageLength())
		payload := make([]byte, length)
		if oversized {
			requireRejectedOfferLeavesTermClean(t, pub, aeron.UnsupportedMessageLength, func() int64 {
				return pub.Offer(payload)
			})
			return
		}
		require.NotEqual(t, aeron.UnsupportedMessageLength, pub.Offer(payload), "length=%d", length)
	})
}

func TestPublication_TryClaim_rapid(t *testing.T) {
	t.Parallel()
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

			termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
			require.NoError(t, pub.Commit(termIndex, frameOffset))

			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameOffset))
			got := termBuf.Bytes()[frameOffset+logbuffer.OffsetFramePayload : frameOffset+frameLength]
			require.Equal(t, payload, got)
		}
	})
}

func TestOfferVariants_matchConcatenatedOffer_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		kind := offerVariantKinds[rapid.IntRange(0, len(offerVariantKinds)-1).Draw(t, "kind")]
		frames := rapid.IntRange(0, aeron.TestPublicationTermLength/logbuffer.FrameAlignment-1).
			Draw(t, "startFrames")
		cfg := aeron.LogConfigAtPositionForTest(
			kind.baseConfig,
			int64(frames*logbuffer.FrameAlignment),
		)
		reference := kind.open(cfg, int64(1)<<62)
		subject := kind.open(cfg, int64(1)<<62)
		maxPayload := reference.MaxPayloadLength()
		maxMessage := reference.MaxMessageLength()

		messageCount := rapid.IntRange(1, 8).Draw(t, "messageCount")
		for i := range messageCount {
			length := rapid.IntRange(0, maxMessage+1).Draw(t, fmt.Sprintf("length%d", i))
			cuts := rapid.SliceOfN(rapid.IntRange(0, length), 0, 6).Draw(t, fmt.Sprintf("cuts%d", i))
			slices.Sort(cuts)
			formIndex := rapid.IntRange(0, len(offerVariantForms)-1).Draw(t, fmt.Sprintf("form%d", i))
			form := offerVariantForms[formIndex]
			withSupplier := rapid.Bool().Draw(t, fmt.Sprintf("supplier%d", i))
			seed := rapid.Byte().Draw(t, fmt.Sprintf("seed%d", i))

			payload := make([]byte, length)
			for j := range payload {
				payload[j] = byte(j) + seed
			}

			recorder := &reservedValueRecorder{tb: t, frameCount: offerFrameCount(length, maxPayload)}
			var supplier aeron.ReservedValueSupplier
			if withSupplier {
				supplier = recorder.supply
			}

			// Both publications retry AdminAction together, so a step also
			// covers term-end padding and rotation.
			var want int64
			for {
				recorder.frames = recorder.frames[:0]
				want = reference.OfferWithOffset(payload, 0, length)
				got := form.offer(subject, payload, cuts, supplier)
				require.Equal(t, want, got)
				require.Equal(t, reference.Position(), subject.Position())

				if withSupplier && want > 0 {
					require.Len(t, recorder.frames, recorder.frameCount, "supplier calls")
				} else {
					require.Empty(t, recorder.frames, "supplier calls")
				}
				// The frames of one message sit back to back in one term.
				for frameIndex := 1; frameIndex < len(recorder.frames); frameIndex++ {
					previous, frame := recorder.frames[frameIndex-1], recorder.frames[frameIndex]
					require.Equal(t, previous.termID, frame.termID, "frame %d term", frameIndex)
					require.Equal(t,
						previous.termOffset+logbuffer.AlignFrameLength(previous.frameLength),
						frame.termOffset,
						"frame %d offset", frameIndex,
					)
				}
				for _, frame := range recorder.frames {
					termCount := logbuffer.ComputeTermCount(frame.termID, aeron.TestPublicationInitialTermID)
					term := reference.TermBufferForTest(logbuffer.IndexByTermCount(termCount))
					putExpectedReservedValue(term, frame.termOffset)
				}
				requireEqualTerms(t, reference, subject)

				if want != aeron.AdminAction {
					break
				}
			}
			if length > maxMessage {
				require.Equal(t, aeron.UnsupportedMessageLength, want)
			} else {
				require.Positive(t, want)
			}
		}
	})
}

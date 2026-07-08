package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

const termRotationTestMargin = 64

func publicationLogConfigNearTermEnd(margin int) aeron.TestPublicationLogConfig {
	return aeron.LogConfigNearTermEndForTest(aeron.DefaultTestPublicationLogConfig, margin)
}

func requireEndOfTermPaddingOnBuffer(
	tb testing.TB,
	termBuf atomicbuffer.AtomicBuffer,
	termLength, margin int,
) {
	tb.Helper()

	paddingOffset := termLength - margin
	require.True(tb, logbuffer.IsPaddingFrame(termBuf, paddingOffset))
	require.Equal(tb, int32(margin), logbuffer.FrameLengthVolatile(termBuf, paddingOffset))
}

func requireRotatedToNextTermMeta(
	tb testing.TB,
	meta atomicbuffer.AtomicBuffer,
	termLength int,
	wantTermCount int32,
) int {
	tb.Helper()

	require.Equal(tb, wantTermCount, logbuffer.ActiveTermCount(meta))

	nextIndex := logbuffer.IndexByTermCount(wantTermCount)
	rawTail, err := logbuffer.GetRawTail(meta, nextIndex)
	require.NoError(tb, err)
	wantTermID := int32(aeron.TestPublicationInitialTermID + wantTermCount)
	require.Equal(tb, wantTermID, logbuffer.TermID(rawTail))
	require.Equal(tb, int32(0), logbuffer.TermOffset(rawTail, int32(termLength)))
	return nextIndex
}

func requireEndOfTermPaddingFrame(
	t *testing.T,
	pub *aeron.Publication,
	termIndex, termLength, margin int,
) {
	t.Helper()

	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	requireEndOfTermPaddingOnBuffer(t, termBuf, termLength, margin)
}

func requirePublicationRotatedToNextTerm(t *testing.T, pub *aeron.Publication, termLength int) int {
	t.Helper()

	return requireRotatedToNextTermMeta(t, pub.MetaDataBufferForTest(), termLength, 1)
}

func requireDataFramePayloadAtOffset(
	t *testing.T,
	pub *aeron.Publication,
	termIndex, frameOffset int,
	wantPayload []byte,
) {
	t.Helper()

	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	nextTermID := int32(aeron.TestPublicationInitialTermID + 1)
	require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(termBuf, frameOffset))
	require.Equal(t, nextTermID, logbuffer.FrameTermID(termBuf, frameOffset))
	require.Equal(t, int32(frameOffset), logbuffer.FrameTermOffset(termBuf, frameOffset))
	start := frameOffset + logbuffer.OffsetFramePayload
	require.Equal(t, wantPayload, termBuf.Bytes()[start:start+len(wantPayload)])
}

func TestVerifyPublicationLogIntegrityForTest_nearTermEndAfterOffer(t *testing.T) {
	margin := 256
	pub := aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(margin), int64(1)<<62)

	position := pub.Offer([]byte("hello"))
	require.Positive(t, position)

	aeron.VerifyPublicationLogIntegrityForTest(t, pub)
}

func TestPublication_TermRotation_OnEndOfTerm(t *testing.T) {
	termLength := aeron.TestPublicationTermLength
	pub := newTestPublicationNearTermEnd(t, termRotationTestMargin)

	first := pub.Offer(make([]byte, termRotationTestMargin))
	require.Equal(t, aeron.AdminAction, first)

	requireEndOfTermPaddingFrame(t, pub, 0, termLength, termRotationTestMargin)
	nextIndex := requirePublicationRotatedToNextTerm(t, pub, termLength)

	second := pub.Offer([]byte{9, 8, 7})
	require.Positive(t, second)

	requireDataFramePayloadAtOffset(t, pub, nextIndex, 0, []byte{9, 8, 7})

	for range 3 {
		position := aeron.OfferRetryForTest(pub, []byte{1, 2, 3, 4, 5, 6, 7, 8})
		require.Positive(t, position)
		require.NotEqual(t, aeron.BackPressured, position)
	}
}

func TestPublication_TermRotation_TryClaim(t *testing.T) {
	termLength := aeron.TestPublicationTermLength
	pub := newTestPublicationNearTermEnd(t, termRotationTestMargin)

	_, _, position, err := pub.TryClaim(termRotationTestMargin)
	require.NoError(t, err)
	require.Equal(t, aeron.AdminAction, position)

	requireEndOfTermPaddingFrame(t, pub, 0, termLength, termRotationTestMargin)
	nextIndex := requirePublicationRotatedToNextTerm(t, pub, termLength)

	termIndex, frameOffset, position, err := pub.TryClaim(8)
	require.NoError(t, err)
	require.Positive(t, position)
	require.Equal(t, nextIndex, termIndex)
	require.Equal(t, 0, frameOffset)

	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("12345678"))
	require.NoError(t, pub.Commit(termIndex, frameOffset))
	require.Equal(
		t,
		int32(8+logbuffer.DataHeaderLength),
		logbuffer.FrameLengthVolatile(termBuf, frameOffset),
	)
}

func TestPublication_TermRotation_TwoStepNearTermEnd(t *testing.T) {
	termLength := aeron.TestPublicationTermLength
	firstOffset := termLength - termRotationTestMargin

	t.Run("Offer", func(t *testing.T) {
		pub := newTestPublicationNearTermEnd(t, termRotationTestMargin)

		firstPayload := make([]byte, 16)
		for i := range firstPayload {
			firstPayload[i] = byte(i)
		}
		first := pub.Offer(firstPayload)
		require.Positive(t, first)
		require.NotEqual(t, aeron.AdminAction, first)

		termBuf := aeron.PublicationTermBufferForTest(pub, 0)
		require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(termBuf, firstOffset))
		firstPayloadStart := firstOffset + logbuffer.OffsetFramePayload
		gotFirst := termBuf.Bytes()[firstPayloadStart : firstPayloadStart+16]
		require.Equal(t, firstPayload, gotFirst)

		second := pub.Offer(make([]byte, termRotationTestMargin))
		require.Equal(t, aeron.AdminAction, second)

		require.False(t, logbuffer.IsPaddingFrame(termBuf, firstOffset))
		require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termLength))

		nextIndex := requirePublicationRotatedToNextTerm(t, pub, termLength)

		third := pub.Offer([]byte{9, 8, 7})
		require.Positive(t, third)

		requireDataFramePayloadAtOffset(t, pub, nextIndex, 0, []byte{9, 8, 7})
	})

	t.Run("TryClaim+Commit", func(t *testing.T) {
		pub := newTestPublicationNearTermEnd(t, termRotationTestMargin)

		termIndex, frameOffset, position, err := pub.TryClaim(16)
		require.NoError(t, err)
		require.Positive(t, position)
		require.NotEqual(t, aeron.AdminAction, position)
		require.Equal(t, 0, termIndex)
		require.Equal(t, firstOffset, frameOffset)

		termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("abcdefghijklmnop"))
		require.NoError(t, pub.Commit(termIndex, frameOffset))
		require.Equal(
			t,
			int32(16+logbuffer.DataHeaderLength),
			logbuffer.FrameLengthVolatile(termBuf, frameOffset),
		)

		_, _, position, err = pub.TryClaim(termRotationTestMargin)
		require.NoError(t, err)
		require.Equal(t, aeron.AdminAction, position)

		require.False(t, logbuffer.IsPaddingFrame(termBuf, firstOffset))
		require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termLength))

		nextIndex := requirePublicationRotatedToNextTerm(t, pub, termLength)

		termIndex, frameOffset, position, err = pub.TryClaim(8)
		require.NoError(t, err)
		require.Positive(t, position)
		require.Equal(t, nextIndex, termIndex)
		require.Equal(t, 0, frameOffset)

		nextTermBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		nextTermBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("12345678"))
		require.NoError(t, pub.Commit(termIndex, frameOffset))
		require.Equal(
			t,
			int32(8+logbuffer.DataHeaderLength),
			logbuffer.FrameLengthVolatile(nextTermBuf, frameOffset),
		)
	})
}

package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func publicationLogConfigNearTermEnd(margin int) aeron.TestPublicationLogConfig {
	cfg := aeron.DefaultTestPublicationLogConfig()
	cfg.RawTails[0] = logbuffer.PackRawTail(
		aeron.TestPublicationInitialTermID,
		int32(aeron.TestPublicationTermLength-margin),
	)
	return cfg
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
	margin := 64
	pub := aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(margin), int64(1)<<62)

	first := pub.Offer(make([]byte, margin))
	require.Equal(t, aeron.AdminAction, first)

	termBuf := aeron.PublicationTermBufferForTest(pub, 0)
	paddingOffset := termLength - margin
	require.True(t, logbuffer.IsPaddingFrame(termBuf, paddingOffset))
	require.Equal(t, int32(margin), logbuffer.FrameLengthVolatile(termBuf, paddingOffset))

	require.Equal(t, int32(1), logbuffer.ActiveTermCount(pub.MetaDataBufferForTest()))

	nextIndex := logbuffer.IndexByTermCount(1)
	rawTail, err := logbuffer.GetRawTail(pub.MetaDataBufferForTest(), nextIndex)
	require.NoError(t, err)
	require.Equal(t, int32(aeron.TestPublicationInitialTermID+1), logbuffer.TermID(rawTail))
	require.Equal(t, int32(0), logbuffer.TermOffset(rawTail, int32(termLength)))

	second := pub.Offer([]byte{9, 8, 7})
	require.Positive(t, second)

	nextTermBuf := aeron.PublicationTermBufferForTest(pub, nextIndex)
	require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(nextTermBuf, 0))
	nextTermID := int32(aeron.TestPublicationInitialTermID + 1)
	require.Equal(t, nextTermID, logbuffer.FrameTermID(nextTermBuf, 0))
	require.Equal(t, int32(0), logbuffer.FrameTermOffset(nextTermBuf, 0))
	gotPayload := nextTermBuf.Bytes()[logbuffer.DataHeaderLength : logbuffer.DataHeaderLength+3]
	require.Equal(t, []byte{9, 8, 7}, gotPayload)

	for range 3 {
		position := aeron.OfferRetryForTest(pub, []byte{1, 2, 3, 4, 5, 6, 7, 8})
		require.Positive(t, position)
		require.NotEqual(t, aeron.BackPressured, position)
	}
}

func TestPublication_TermRotation_TryClaim(t *testing.T) {
	termLength := aeron.TestPublicationTermLength
	margin := 64
	pub := aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(margin), int64(1)<<62)

	_, _, position, err := pub.TryClaim(margin)
	require.NoError(t, err)
	require.Equal(t, aeron.AdminAction, position)

	termBuf := aeron.PublicationTermBufferForTest(pub, 0)
	paddingOffset := termLength - margin
	require.True(t, logbuffer.IsPaddingFrame(termBuf, paddingOffset))
	require.Equal(t, int32(margin), logbuffer.FrameLengthVolatile(termBuf, paddingOffset))

	require.Equal(t, int32(1), logbuffer.ActiveTermCount(pub.MetaDataBufferForTest()))

	nextIndex := logbuffer.IndexByTermCount(1)
	rawTail, err := logbuffer.GetRawTail(pub.MetaDataBufferForTest(), nextIndex)
	require.NoError(t, err)
	require.Equal(t, int32(aeron.TestPublicationInitialTermID+1), logbuffer.TermID(rawTail))
	require.Equal(t, int32(0), logbuffer.TermOffset(rawTail, int32(termLength)))

	termIndex, frameOffset, position, err := pub.TryClaim(8)
	require.NoError(t, err)
	require.Positive(t, position)
	require.Equal(t, nextIndex, termIndex)
	require.Equal(t, 0, frameOffset)

	termBuf = aeron.PublicationTermBufferForTest(pub, termIndex)
	termBuf.PutBytes(frameOffset+logbuffer.DataHeaderLength, []byte("12345678"))
	require.NoError(t, pub.Commit(termIndex, frameOffset))
	require.Equal(
		t,
		int32(8+logbuffer.DataHeaderLength),
		logbuffer.FrameLengthVolatile(termBuf, frameOffset),
	)
}

func TestPublication_TermRotation_TwoStepNearTermEnd(t *testing.T) {
	termLength := aeron.TestPublicationTermLength
	margin := 64
	firstOffset := termLength - margin

	t.Run("Offer", func(t *testing.T) {
		pub := aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(margin), int64(1)<<62)

		firstPayload := make([]byte, 16)
		for i := range firstPayload {
			firstPayload[i] = byte(i)
		}
		first := pub.Offer(firstPayload)
		require.Positive(t, first)
		require.NotEqual(t, aeron.AdminAction, first)

		termBuf := aeron.PublicationTermBufferForTest(pub, 0)
		require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(termBuf, firstOffset))
		firstPayloadStart := firstOffset + logbuffer.DataHeaderLength
		gotFirst := termBuf.Bytes()[firstPayloadStart : firstPayloadStart+16]
		require.Equal(t, firstPayload, gotFirst)

		second := pub.Offer(make([]byte, margin))
		require.Equal(t, aeron.AdminAction, second)

		require.False(t, logbuffer.IsPaddingFrame(termBuf, firstOffset))
		require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termLength))

		require.Equal(t, int32(1), logbuffer.ActiveTermCount(pub.MetaDataBufferForTest()))

		nextIndex := logbuffer.IndexByTermCount(1)
		rawTail, err := logbuffer.GetRawTail(pub.MetaDataBufferForTest(), nextIndex)
		require.NoError(t, err)
		require.Equal(t, int32(aeron.TestPublicationInitialTermID+1), logbuffer.TermID(rawTail))
		require.Equal(t, int32(0), logbuffer.TermOffset(rawTail, int32(termLength)))

		third := pub.Offer([]byte{9, 8, 7})
		require.Positive(t, third)

		nextTermBuf := aeron.PublicationTermBufferForTest(pub, nextIndex)
		require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(nextTermBuf, 0))
		nextTermID := int32(aeron.TestPublicationInitialTermID + 1)
		require.Equal(t, nextTermID, logbuffer.FrameTermID(nextTermBuf, 0))
		require.Equal(t, int32(0), logbuffer.FrameTermOffset(nextTermBuf, 0))
		gotThird := nextTermBuf.Bytes()[logbuffer.DataHeaderLength : logbuffer.DataHeaderLength+3]
		require.Equal(t, []byte{9, 8, 7}, gotThird)
	})

	t.Run("TryClaim+Commit", func(t *testing.T) {
		pub := aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(margin), int64(1)<<62)

		termIndex, frameOffset, position, err := pub.TryClaim(16)
		require.NoError(t, err)
		require.Positive(t, position)
		require.NotEqual(t, aeron.AdminAction, position)
		require.Equal(t, 0, termIndex)
		require.Equal(t, firstOffset, frameOffset)

		termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		termBuf.PutBytes(frameOffset+logbuffer.DataHeaderLength, []byte("abcdefghijklmnop"))
		require.NoError(t, pub.Commit(termIndex, frameOffset))
		require.Equal(
			t,
			int32(16+logbuffer.DataHeaderLength),
			logbuffer.FrameLengthVolatile(termBuf, frameOffset),
		)

		_, _, position, err = pub.TryClaim(margin)
		require.NoError(t, err)
		require.Equal(t, aeron.AdminAction, position)

		require.False(t, logbuffer.IsPaddingFrame(termBuf, firstOffset))
		require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termLength))

		require.Equal(t, int32(1), logbuffer.ActiveTermCount(pub.MetaDataBufferForTest()))

		nextIndex := logbuffer.IndexByTermCount(1)
		rawTail, err := logbuffer.GetRawTail(pub.MetaDataBufferForTest(), nextIndex)
		require.NoError(t, err)
		require.Equal(t, int32(aeron.TestPublicationInitialTermID+1), logbuffer.TermID(rawTail))
		require.Equal(t, int32(0), logbuffer.TermOffset(rawTail, int32(termLength)))

		termIndex, frameOffset, position, err = pub.TryClaim(8)
		require.NoError(t, err)
		require.Positive(t, position)
		require.Equal(t, nextIndex, termIndex)
		require.Equal(t, 0, frameOffset)

		nextTermBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		nextTermBuf.PutBytes(frameOffset+logbuffer.DataHeaderLength, []byte("12345678"))
		require.NoError(t, pub.Commit(termIndex, frameOffset))
		require.Equal(
			t,
			int32(8+logbuffer.DataHeaderLength),
			logbuffer.FrameLengthVolatile(nextTermBuf, frameOffset),
		)
	})
}

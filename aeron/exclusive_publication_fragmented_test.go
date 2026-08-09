package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// exclusiveFragmentedMessageAtPosition returns the term buffer and start offset
// of a fragmented or single-frame message that ended at position.
func exclusiveFragmentedMessageAtPosition(
	ep *aeron.ExclusivePublication,
	position int64,
	length, maxPayload int,
) (termBuf atomicbuffer.AtomicBuffer, frameStart int) {
	termIndex, frameStart := fragmentedMessageFrameLocation(position, length, maxPayload)
	return aeron.ExclusivePublicationTermBufferForTest(ep, termIndex), frameStart
}

func readExclusiveFragmentedFramesAtPosition(
	tb fragmentTB,
	ep *aeron.ExclusivePublication,
	position int64,
	length, maxPayload int,
) []aeron.FragmentedFrameForTest {
	tb.Helper()

	termBuf, frameStart := exclusiveFragmentedMessageAtPosition(ep, position, length, maxPayload)
	return aeron.ReadFragmentedMessageForTest(tb, termBuf, frameStart)
}

func reassembleExclusiveOfferViaAssembler(
	tb fragmentTB,
	termBuf atomicbuffer.AtomicBuffer,
	frameStart int,
) []byte {
	tb.Helper()

	var got []byte
	assembler := aeron.NewFragmentAssembler(func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		payloadLength int,
		_ *aeron.Header,
	) {
		got = append([]byte(nil), buffer.Bytes()[offset:offset+payloadLength]...)
	})

	header := &aeron.Header{}
	header.SetBuffer(termBuf)
	offset := frameStart
	for range 64 {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
		require.Positive(tb, frameLength)

		payloadOffset := offset + logbuffer.OffsetFramePayload
		payloadLength := frameLength - logbuffer.DataHeaderLength
		header.SetOffset(offset)
		header.ResetPollFragmentState()
		assembler.Handler()(termBuf, payloadOffset, payloadLength, header)

		if logbuffer.FrameFlags(termBuf, offset)&logbuffer.EndFlag != 0 {
			require.NotNil(tb, got, "FragmentAssembler did not deliver END payload")
			return got
		}
		offset += logbuffer.AlignFrameLength(frameLength)
	}

	require.FailNow(tb, "fragmented message missing END flag within iteration budget")
	return nil
}

func requireExclusiveFragmentedOffer(
	tb fragmentTB,
	ep *aeron.ExclusivePublication,
	payload []byte,
) (position int64, frames []aeron.FragmentedFrameForTest) {
	tb.Helper()

	maxPayload := ep.MaxPayloadLengthForTest()
	length := len(payload)
	position = aeron.OfferRetryForTest(ep, payload)
	require.Positive(tb, position)

	termBuf, frameStart := exclusiveFragmentedMessageAtPosition(ep, position, length, maxPayload)
	frames = aeron.ReadFragmentedMessageForTest(tb, termBuf, frameStart)
	requireFragmentedMessageShape(tb, frames, length, maxPayload)
	requireReassembledPayload(tb, frames, payload)
	require.Equal(tb, payload, reassembleExclusiveOfferViaAssembler(tb, termBuf, frameStart))
	return position, frames
}

func TestExclusivePublication_FragmentedOffer_twoFrames(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		int64(1)<<40,
	)
	maxPayload := ep.MaxPayloadLengthForTest()
	length := maxPayload + 1

	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(i)
	}

	_, frames := requireExclusiveFragmentedOffer(t, ep, payload)
	require.Len(t, frames[0].Payload, maxPayload)
	require.Len(t, frames[1].Payload, 1)
}

func TestExclusivePublication_FragmentedOffer_threeFrames(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		int64(1)<<40,
	)
	maxPayload := ep.MaxPayloadLengthForTest()
	length := 3 * maxPayload

	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(0xA0 + (i % 16))
	}

	_, frames := requireExclusiveFragmentedOffer(t, ep, payload)
	for i, frame := range frames {
		require.Len(t, frame.Payload, maxPayload)
		start := i * maxPayload
		require.Equal(t, payload[start:start+maxPayload], frame.Payload)
	}
}

func TestExclusivePublication_FragmentedOffer_atMaxPayloadLength(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		int64(1)<<40,
	)
	maxPayload := ep.MaxPayloadLengthForTest()
	payload := make([]byte, maxPayload)
	for i := range payload {
		payload[i] = byte(i)
	}

	position := aeron.OfferRetryForTest(ep, payload)
	require.Positive(t, position)

	termBuf, frameStart := exclusiveFragmentedMessageAtPosition(ep, position, maxPayload, maxPayload)

	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, frameStart))
	require.Equal(t, maxPayload+logbuffer.DataHeaderLength, frameLength)
	require.Equal(
		t,
		uint8(logbuffer.Unfragmented),
		logbuffer.FrameFlags(termBuf, frameStart)&logbuffer.Unfragmented,
	)
	got := termBuf.Bytes()[frameStart+logbuffer.OffsetFramePayload : frameStart+frameLength]
	require.Equal(t, payload, got)
}

func TestExclusivePublication_FragmentedOffer_atMaxMessageLength(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		int64(1)<<40,
	)
	maxMessage := ep.MaxMessageLengthForTest()
	maxPayload := ep.MaxPayloadLengthForTest()
	require.Greater(t, maxMessage, maxPayload)

	payload := make([]byte, maxMessage)
	for i := range payload {
		payload[i] = byte(i)
	}
	requireExclusiveFragmentedOffer(t, ep, payload)
}

func TestExclusivePublication_FragmentedOffer_termRolloverRetry(t *testing.T) {
	maxPayload := int(aeron.TestPublicationMTULength) - logbuffer.DataHeaderLength
	margin := logbuffer.AlignFrameLength(maxPayload + logbuffer.DataHeaderLength)
	ep := newTestExclusivePublicationNearTermEnd(t, margin)

	length := maxPayload + 1
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}

	first := ep.Offer(payload)
	require.Equal(t, aeron.AdminAction, first)
	require.Equal(t, int32(1), logbuffer.ActiveTermCount(ep.MetaDataBufferForTest()))

	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, 0)
	paddingOffset := aeron.TestPublicationTermLength - margin
	require.True(t, logbuffer.IsPaddingFrame(termBuf, paddingOffset))
	require.Equal(t, int32(margin), logbuffer.FrameLengthVolatile(termBuf, paddingOffset))

	position := aeron.OfferRetryForTest(ep, payload)
	require.Positive(t, position)

	shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
	require.NotEqual(t, 0, logbuffer.IndexByPosition(position, shift))

	frames := readExclusiveFragmentedFramesAtPosition(t, ep, position, length, maxPayload)
	requireFragmentedMessageShape(t, frames, length, maxPayload)
	requireReassembledPayload(t, frames, payload)
}

func TestExclusivePublication_FragmentedOffer_withOffset(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		int64(1)<<40,
	)
	maxPayload := ep.MaxPayloadLengthForTest()
	length := maxPayload + 1

	buffer := append([]byte("xx"), make([]byte, length)...)
	for i := 2; i < len(buffer); i++ {
		buffer[i] = byte(i)
	}

	position := aeron.OfferWithOffsetRetryForTest(ep, buffer, 2, length)
	require.Positive(t, position)

	frames := readExclusiveFragmentedFramesAtPosition(t, ep, position, length, maxPayload)
	requireFragmentedMessageShape(t, frames, length, maxPayload)
	require.Len(t, frames[0].Payload, maxPayload)
	require.Len(t, frames[1].Payload, 1)
	requireReassembledPayload(t, frames, buffer[2:])
}

func TestExclusivePublication_FragmentedOffer_statusPaths(t *testing.T) {
	fragmentedLength := func(ep *aeron.ExclusivePublication) int {
		return ep.MaxPayloadLengthForTest() + 1
	}

	t.Run("BackPressured", func(t *testing.T) {
		ep, _ := setupExclusivePublicationWithLimitReaderAtPosition(t, 500_000, 500_000)
		payload := make([]byte, fragmentedLength(ep))
		requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.BackPressured, func() int64 {
			return ep.Offer(payload)
		})
	})

	t.Run("NotConnected", func(t *testing.T) {
		cfg := aeron.DefaultTestExclusivePublicationLogConfig()
		cfg.IsConnected = 0
		ep := aeron.NewTestExclusivePublication(t, cfg, 0)
		payload := make([]byte, fragmentedLength(ep))
		requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.NotConnected, func() int64 {
			return ep.Offer(payload)
		})
	})

	t.Run("Closed", func(t *testing.T) {
		ep := aeron.NewTestExclusivePublication(
			t,
			aeron.DefaultTestExclusivePublicationLogConfig(),
			1_000_000,
		)
		require.NoError(t, ep.Close())
		got := ep.Offer(make([]byte, fragmentedLength(ep)))
		require.Equal(t, aeron.Closed, got)
	})

	t.Run("over maxMessageLength term clean", func(t *testing.T) {
		ep := aeron.NewTestExclusivePublication(
			t,
			aeron.DefaultTestExclusivePublicationLogConfig(),
			int64(1)<<40,
		)
		maxMessage := ep.MaxMessageLengthForTest()
		payload := make([]byte, maxMessage+1)
		requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.MaxPositionExceeded, func() int64 {
			return ep.Offer(payload)
		})
	})
}

func TestExclusivePublication_MaxPositionExceeded_fragmented(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.LogConfigNearMaxPositionForTest(aeron.DefaultTestExclusivePublicationLogConfig),
		int64(1)<<62,
	)
	maxPayload := ep.MaxPayloadLengthForTest()
	requireRejectedExclusiveOfferLeavesTermClean(t, ep, aeron.MaxPositionExceeded, func() int64 {
		return ep.Offer(make([]byte, maxPayload+1))
	})
}

package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func readFragmentedFramesAtPosition(
	tb testing.TB,
	pub *aeron.Publication,
	position int64,
	length, maxPayload int,
) []aeron.FragmentedFrameForTest {
	tb.Helper()

	shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
	totalAligned := logbuffer.ComputeFragmentedFrameLength(length, maxPayload)
	frameStart := aeron.FragmentedMessageStartOffsetForTest(position, shift, totalAligned)
	termIndex := logbuffer.IndexByPosition(position, shift)
	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	return aeron.ReadFragmentedMessageForTest(tb, termBuf, frameStart)
}

func requireReassembledPayload(
	tb testing.TB,
	frames []aeron.FragmentedFrameForTest,
	want []byte,
) {
	tb.Helper()
	require.Equal(tb, want, reassembledPayload(frames))
}

func reassembledPayload(frames []aeron.FragmentedFrameForTest) []byte {
	var got []byte
	for _, frame := range frames {
		got = append(got, frame.Payload...)
	}
	return got
}

func collectCompleteFragmentedPayloads(
	tb testing.TB,
	pub *aeron.Publication,
	maxPayload int,
) [][]byte {
	tb.Helper()

	var payloads [][]byte
	for partition := range logbuffer.PartitionCount {
		rawTail, err := logbuffer.GetRawTail(pub.MetaDataBufferForTest(), partition)
		require.NoError(tb, err)

		termEnd := int(logbuffer.TermOffset(rawTail, aeron.TestPublicationTermLength))
		termBuf := aeron.PublicationTermBufferForTest(pub, partition)
		offset := 0
		for offset < termEnd {
			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
			if frameLength <= 0 {
				offset += logbuffer.FrameAlignment
				continue
			}

			absLength := frameLength
			if absLength < 0 {
				absLength = -absLength
			}
			aligned := logbuffer.AlignFrameLength(absLength)

			if logbuffer.IsPaddingFrame(termBuf, offset) {
				offset += aligned
				continue
			}

			flags := logbuffer.FrameFlags(termBuf, offset)
			if flags&logbuffer.Unfragmented == logbuffer.Unfragmented {
				offset += aligned
				continue
			}

			if flags&logbuffer.BeginFlag != 0 && flags&logbuffer.EndFlag == 0 {
				frames, ok := aeron.ReadFragmentedMessageFramesForTest(termBuf, offset)
				if ok {
					payload := reassembledPayload(frames)
					requireFragmentFlags(tb, frames)
					payloads = append(payloads, payload)
					offset += logbuffer.ComputeFragmentedFrameLength(len(payload), maxPayload)
					continue
				}
			}

			offset += aligned
		}
	}

	return payloads
}

func requireCommittedFragmentedPayloads(
	tb testing.TB,
	pub *aeron.Publication,
	want []byte,
) {
	tb.Helper()

	maxPayload := pub.MaxPayloadLengthForTest()
	got := collectCompleteFragmentedPayloads(tb, pub, maxPayload)
	require.NotEmpty(
		tb,
		got,
		"expected at least one committed fragmented message in the log",
	)
	for _, payload := range got {
		require.Equal(tb, want, payload)
	}
}

func requireFragmentFlags(tb require.TestingT, frames []aeron.FragmentedFrameForTest) {
	if helper, ok := tb.(interface{ Helper() }); ok {
		helper.Helper()
	}

	for i, frame := range frames {
		switch i {
		case 0:
			require.Equal(tb, uint8(logbuffer.BeginFlag), frame.Flags&logbuffer.BeginFlag)
		case len(frames) - 1:
			require.Equal(tb, uint8(logbuffer.EndFlag), frame.Flags&logbuffer.EndFlag)
		default:
			require.Equal(tb, uint8(0), frame.Flags&(logbuffer.BeginFlag|logbuffer.EndFlag))
		}
	}
}

func TestPublication_FragmentedOffer_twoFrames(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<40)
	maxPayload := pub.MaxPayloadLengthForTest()
	length := maxPayload + 1

	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(i)
	}

	position := aeron.OfferRetryForTest(pub, payload)
	require.Positive(t, position)

	frames := readFragmentedFramesAtPosition(t, pub, position, length, maxPayload)

	require.Len(t, frames, 2)
	require.Equal(t, uint8(0), frames[0].Flags&logbuffer.EndFlag)
	require.Equal(t, uint8(0), frames[1].Flags&logbuffer.BeginFlag)
	require.Len(t, frames[0].Payload, maxPayload)
	require.Len(t, frames[1].Payload, 1)
	requireReassembledPayload(t, frames, payload)
}

func TestPublication_FragmentedOffer_threeFrames(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<40)
	maxPayload := pub.MaxPayloadLengthForTest()
	length := 3 * maxPayload

	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(0xA0 + (i % 16))
	}

	position := aeron.OfferRetryForTest(pub, payload)
	require.Positive(t, position)

	frames := readFragmentedFramesAtPosition(t, pub, position, length, maxPayload)

	require.Len(t, frames, 3)
	requireFragmentFlags(t, frames)

	for i, frame := range frames {
		require.Len(t, frame.Payload, maxPayload)
		start := i * maxPayload
		require.Equal(t, payload[start:start+maxPayload], frame.Payload)
	}
}

func TestPublication_FragmentedOffer_termRolloverRetry(t *testing.T) {
	maxPayload := int(aeron.TestPublicationMTULength) - logbuffer.DataHeaderLength
	margin := logbuffer.AlignFrameLength(maxPayload + logbuffer.DataHeaderLength)
	pub := newTestPublicationNearTermEnd(t, margin)

	length := maxPayload + 1
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}

	first := pub.Offer(payload)
	require.Equal(t, aeron.AdminAction, first)
	require.Equal(t, int32(1), logbuffer.ActiveTermCount(pub.MetaDataBufferForTest()))

	termBuf := aeron.PublicationTermBufferForTest(pub, 0)
	paddingOffset := aeron.TestPublicationTermLength - margin
	require.True(t, logbuffer.IsPaddingFrame(termBuf, paddingOffset))
	require.Equal(t, int32(margin), logbuffer.FrameLengthVolatile(termBuf, paddingOffset))
	require.Equal(
		t,
		int32(0),
		logbuffer.FrameLengthVolatile(termBuf, paddingOffset+logbuffer.DataHeaderLength),
	)

	position := aeron.OfferRetryForTest(pub, payload)
	require.Positive(t, position)

	shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
	require.NotEqual(
		t,
		0,
		logbuffer.IndexByPosition(position, shift),
	)

	frames := readFragmentedFramesAtPosition(t, pub, position, length, maxPayload)
	require.Len(t, frames, 2)
	requireFragmentFlags(t, frames)
	requireReassembledPayload(t, frames, payload)
}

func TestPublication_FragmentedOffer_midFragmentTermIDChangeAdminAction(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<40)
	maxPayload := pub.MaxPayloadLengthForTest()
	length := maxPayload*2 + 1

	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(0xB0 + (i % 16))
	}

	framedLength := logbuffer.ComputeFragmentedFrameLength(length, maxPayload)
	var (
		reservationStart   int
		paddingStart       int
		reservationEnd     int
		termIndex          int
		gotReservation     bool
		bumpedTermID       bool
		firstFragmentEnded bool
	)
	aeron.SetFragmentedWriteTrackForTest(func(ti, frameOffset int, begin bool) {
		if begin {
			if !gotReservation {
				gotReservation = true
				reservationStart = frameOffset
				termIndex = ti
				reservationEnd = frameOffset + framedLength
			}
			return
		}
		if !gotReservation || firstFragmentEnded {
			return
		}
		firstFragmentEnded = true

		paddingStart = frameOffset + logbuffer.AlignFragmentChunkLength(maxPayload)
		rawTail, err := logbuffer.GetRawTail(pub.MetaDataBufferForTest(), ti)
		require.NoError(t, err)
		termID := logbuffer.TermID(rawTail)
		termOffset := logbuffer.TermOffset(rawTail, aeron.TestPublicationTermLength)
		aeron.SetPublicationRawTailForTest(
			pub,
			ti,
			logbuffer.PackRawTail(termID+1, termOffset),
		)
		bumpedTermID = true
	})
	t.Cleanup(func() { aeron.SetFragmentedWriteTrackForTest(nil) })

	first := pub.Offer(payload)
	require.Equal(t, aeron.AdminAction, first)
	require.True(t, gotReservation)
	require.True(t, bumpedTermID)
	require.True(t, firstFragmentEnded)
	require.Positive(t, paddingStart)
	require.Positive(t, reservationEnd)

	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	flags := logbuffer.FrameFlags(termBuf, reservationStart)
	require.Equal(t, uint8(logbuffer.BeginFlag), flags&logbuffer.BeginFlag)
	require.Equal(t, uint8(0), flags&logbuffer.EndFlag)
	require.True(t, logbuffer.IsPaddingFrame(termBuf, paddingStart))
	require.Equal(
		t,
		int32(reservationEnd-paddingStart),
		logbuffer.FrameLengthVolatile(termBuf, paddingStart),
	)

	nextTermCount := int32(1)
	nextTermID := aeron.TestPublicationInitialTermID + 1
	nextIndex := logbuffer.IndexByTermCount(nextTermCount)
	aeron.SetPublicationRawTailForTest(
		pub,
		nextIndex,
		logbuffer.PackRawTail(nextTermID, 0),
	)
	aeron.SetPublicationActiveTermCountForTest(pub, nextTermCount)

	position := aeron.OfferRetryForTest(pub, payload)
	require.Positive(t, position)

	frames := readFragmentedFramesAtPosition(t, pub, position, length, maxPayload)
	require.Len(t, frames, 3)
	requireFragmentFlags(t, frames)
	requireReassembledPayload(t, frames, payload)
}

func TestPublication_FragmentedOffer_withOffset(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<40)
	maxPayload := pub.MaxPayloadLengthForTest()

	buffer := append([]byte("xx"), make([]byte, maxPayload+1)...)
	for i := 2; i < len(buffer); i++ {
		buffer[i] = byte(i)
	}

	position := aeron.OfferWithOffsetRetryForTest(pub, buffer, 2, maxPayload+1)
	require.Positive(t, position)

	frames := readFragmentedFramesAtPosition(t, pub, position, maxPayload+1, maxPayload)
	requireReassembledPayload(t, frames, buffer[2:])
}

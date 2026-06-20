//go:build rapid

package aeron_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

type drawnCommittedFrame struct {
	frameOffset int
	termOffset  int32
	payload     []byte
	frameLength int32
}

func headerRapidImage(t *testing.T) *aeron.Image {
	return testImage(t, testSubscription(t))
}

func drawByteSlice(rt *rapid.T, name string, minLen, maxLen int) []byte {
	if minLen > maxLen {
		panic(fmt.Sprintf("drawByteSlice %q: minLen %d > maxLen %d", name, minLen, maxLen))
	}
	n := rapid.IntRange(minLen, maxLen).Draw(rt, name+"Len")
	return rapid.SliceOfN(rapid.Byte(), n, n).Draw(rt, name)
}

func drawCommittedFrame(
	rt *rapid.T,
	termLength int,
	maxPayloadCap int,
) drawnCommittedFrame {
	maxAlignedSlots := termLength / logbuffer.FrameAlignment
	slot := rapid.IntRange(0, maxAlignedSlots-1).Draw(rt, "frameOffsetSlot")
	frameOffset := slot * logbuffer.FrameAlignment
	maxPayload := termLength - frameOffset - logbuffer.DataHeaderLength
	if maxPayloadCap >= 0 && maxPayload > maxPayloadCap {
		maxPayload = maxPayloadCap
	}
	payload := drawByteSlice(rt, "payload", 0, maxPayload)
	return drawnCommittedFrame{
		frameOffset: frameOffset,
		termOffset:  int32(frameOffset),
		payload:     payload,
		frameLength: int32(len(payload) + logbuffer.DataHeaderLength),
	}
}

func TestHeader_positionMath_rapid(t *testing.T) {
	image := headerRapidImage(t)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	termLength := aeron.TestPublicationTermLength
	shift := logbuffer.PositionBitsToShift(termLength)

	rapid.Check(t, func(rt *rapid.T) {
		aeron.HeaderSetFragmentedFrameLenForTest(aeron.ImageHeaderForTest(image), -1)

		frame := drawCommittedFrame(rt, termLength, -1)
		aeron.WriteCommittedDataFrameForTest(
			termBuffer,
			frame.frameOffset,
			aeron.TestPublicationSessionID,
			aeron.TestPublicationStreamID,
			aeron.TestPublicationInitialTermID,
			frame.termOffset,
			logbuffer.Unfragmented,
			frame.payload,
		)

		header := aeron.ImageHeaderForTest(image)
		header.SetBuffer(termBuffer)
		header.SetOffset(frame.frameOffset)

		useOverride := rapid.Bool().Draw(rt, "useOverride")
		occupancy := frame.frameLength
		if useOverride {
			fragmentedSpan := rapid.Int32Range(1, 4096).Draw(rt, "fragmentedSpan")
			aeron.HeaderSetFragmentedFrameLenForTest(header, fragmentedSpan)
			occupancy = fragmentedSpan
		}

		nextTermOffset := header.NextTermOffset()
		require.Equal(
			rt,
			int32(logbuffer.AlignFrameLength(int(header.TermOffset())+int(occupancy))),
			nextTermOffset,
		)
		require.Equal(
			rt,
			logbuffer.ComputePosition(
				header.TermID(),
				nextTermOffset,
				shift,
				aeron.TestPublicationInitialTermID,
			),
			header.Position(),
		)
	})
}

func drawBeginEndFragmentSpecs(rt *rapid.T) []aeron.FragmentedFrameForTest {
	return []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: drawByteSlice(rt, "beginPayload", 1, 64)},
		{Flags: logbuffer.EndFlag, Payload: drawByteSlice(rt, "endPayload", 0, 64)},
	}
}

func TestHeader_ConfigureCompletedFragment_rapid(t *testing.T) {
	image := headerRapidImage(t)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)

	rapid.Check(t, func(rt *rapid.T) {
		fragments := drawBeginEndFragmentSpecs(rt)
		frameOffsets := aeron.WriteCommittedFragmentedDataFramesForTest(termBuffer, 0, fragments)
		fixture := newCompletedFragmentFixture(image, termBuffer, frameOffsets, fragments)

		assertConfigureCompletedFragmentOracle(rt, fixture, false)
	})
}

func TestHeader_frameFieldsRoundtrip_rapid(t *testing.T) {
	image := headerRapidImage(t)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	termLength := aeron.TestPublicationTermLength

	rapid.Check(t, func(rt *rapid.T) {
		frame := drawCommittedFrame(rt, termLength, 256)
		sessionID := rapid.Int32().Draw(rt, "sessionID")
		streamID := rapid.Int32().Draw(rt, "streamID")
		termID := rapid.Int32().Draw(rt, "termID")
		flags := rapid.Uint8().Draw(rt, "flags")

		aeron.WriteCommittedDataFrameForTest(
			termBuffer,
			frame.frameOffset,
			sessionID,
			streamID,
			termID,
			frame.termOffset,
			flags,
			frame.payload,
		)
		writtenFlags := flags | logbuffer.Unfragmented

		header := aeron.ImageHeaderForTest(image)
		header.SetBuffer(termBuffer)
		header.SetOffset(frame.frameOffset)

		require.Equal(rt, sessionID, header.SessionID())
		require.Equal(rt, streamID, header.StreamID())
		require.Equal(rt, termID, header.TermID())
		require.Equal(rt, frame.termOffset, header.TermOffset())
		require.Equal(rt, int32(logbuffer.HdrTypeData), header.Type())
		require.Equal(rt, byte(writtenFlags), header.Flags())
		require.Equal(rt, int64(logbuffer.DefaultReservedValue), header.ReservedValue())
		require.Equal(rt, frame.frameLength, header.FrameLength())
		require.Equal(rt, frame.frameOffset, header.Offset())
	})
}

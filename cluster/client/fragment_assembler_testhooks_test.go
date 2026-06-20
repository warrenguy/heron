//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestFragmentAssembler_completedHeaderPosition(t *testing.T) {
	beginPayload := []byte("compact")
	endPayload := []byte("end")
	capture, assembler, termBuffer, offsets := setupFragmentAssembler(t, 1024, []fragmentedFrameSpec{
		{flags: logbuffer.BeginFlag, payload: beginPayload},
		{flags: logbuffer.EndFlag, payload: endPayload},
	})

	firstFrameLength := logbuffer.FrameLengthVolatile(termBuffer, offsets[0])
	assembledLen := len(beginPayload) + len(endPayload)
	maxPayloadLength := int(firstFrameLength) - logbuffer.DataHeaderLength
	expectedSpan := int32(logbuffer.ComputeFragmentedFrameLength(assembledLen, maxPayloadLength))
	beginTermOffset := logbuffer.FrameTermOffset(termBuffer, offsets[0])
	expectedNextTermOffset := int32(
		logbuffer.AlignFrameLength(int(beginTermOffset) + int(expectedSpan)),
	)
	// Synthetic term buffers omit Image flyweight metadata; END header ImageFields are zero.
	expectedPosition := logbuffer.ComputePosition(
		logbuffer.FrameTermID(termBuffer, offsets[0]),
		expectedNextTermOffset,
		0,
		0,
	)
	beginFlags := logbuffer.FrameFlags(termBuffer, offsets[0])
	endFlags := logbuffer.FrameFlags(termBuffer, offsets[1])

	deliverFragments(t, assembler, termBuffer, offsets)

	require.Len(t, capture.headers, 1)
	header := capture.headers[0]
	require.Equal(t, expectedSpan, aeron.HeaderFragmentedFrameLenForTest(header))
	require.Equal(t, expectedNextTermOffset, header.NextTermOffset())
	require.Equal(t, expectedPosition, header.Position())
	require.Equal(
		t,
		int32(logbuffer.DataHeaderLength+assembledLen),
		header.FrameLength(),
	)
	require.Equal(t, byte(beginFlags|endFlags), header.Flags())
	require.Equal(
		t,
		logbuffer.FrameSessionID(termBuffer, offsets[0]),
		header.SessionID(),
	)
	require.Equal(
		t,
		logbuffer.FrameStreamID(termBuffer, offsets[0]),
		header.StreamID(),
	)
	require.Equal(
		t,
		logbuffer.FrameTermID(termBuffer, offsets[0]),
		header.TermID(),
	)
	require.Equal(
		t,
		logbuffer.FrameTermOffset(termBuffer, offsets[0]),
		header.TermOffset(),
	)
	require.Equal(t, int32(logbuffer.HdrTypeData), header.Type())
	require.Equal(
		t,
		logbuffer.FrameReservedValue(termBuffer, offsets[0]),
		header.ReservedValue(),
	)
}

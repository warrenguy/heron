//go:build testhooks

package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestFragmentAssembler_completedHeaderPosition(t *testing.T) {
	beginPayload := []byte("compact")
	endPayload := []byte("end")
	specs := []FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: beginPayload},
		{Flags: logbuffer.EndFlag, Payload: endPayload},
	}
	capture, assembler, termBuffer, offsets := setupFragmentAssembler(
		t,
		CommittedDataFramesTermCapacityForTest(specs),
		specs,
	)

	beginOffset := offsets[0]
	endOffset := offsets[1]
	assembledLen := len(beginPayload) + len(endPayload)

	firstFrameLength := logbuffer.FrameLengthVolatile(termBuffer, beginOffset)
	expectedSpan := int32(logbuffer.ComputeFragmentedFrameLength(
		assembledLen,
		int(firstFrameLength)-logbuffer.DataHeaderLength,
	))
	beginTermOffset := logbuffer.FrameTermOffset(termBuffer, beginOffset)
	expectedNextTermOffset := int32(
		logbuffer.AlignFrameLength(int(beginTermOffset) + int(expectedSpan)),
	)
	// Synthetic term buffers omit Image flyweight metadata; END header ImageFields are zero.
	expectedPosition := logbuffer.ComputePosition(
		logbuffer.FrameTermID(termBuffer, beginOffset),
		int(expectedNextTermOffset),
		0,
		0,
	)
	expectedFlags := logbuffer.FrameFlags(termBuffer, beginOffset) |
		logbuffer.FrameFlags(termBuffer, endOffset)

	deliverFragments(t, assembler, termBuffer, offsets)

	require.Len(t, capture.headers, 1)
	header := capture.headers[0]
	require.Equal(t, expectedSpan, HeaderFragmentedFrameLenForTest(header))
	require.Equal(t, expectedNextTermOffset, header.NextTermOffset())
	require.Equal(t, expectedPosition, header.Position())
	require.Equal(t, int32(logbuffer.DataHeaderLength+assembledLen), header.FrameLength())
	require.Equal(t, byte(expectedFlags), header.Flags())
	require.Equal(t, logbuffer.FrameSessionID(termBuffer, beginOffset), header.SessionID())
	require.Equal(t, logbuffer.FrameStreamID(termBuffer, beginOffset), header.StreamID())
	require.Equal(t, logbuffer.FrameTermID(termBuffer, beginOffset), header.TermID())
	require.Equal(t, beginTermOffset, header.TermOffset())
	require.Equal(t, int32(logbuffer.HdrTypeData), header.Type())
	require.Equal(t, logbuffer.FrameReservedValue(termBuffer, beginOffset), header.ReservedValue())
}

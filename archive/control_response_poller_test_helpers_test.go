//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func setupControlResponsePollFixtures(
	t testing.TB,
) (*aeron.Subscription, atomicbuffer.AtomicBuffer) {
	t.Helper()

	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return sub, aeron.ImageTermBufferForHooks(image)
}

func newControlResponsePollerForTest(sub *aeron.Subscription) *ControlResponsePoller {
	return NewControlResponsePoller(sub, DefaultConnectControlResponseFragmentLimit)
}

func writeUnfragmentedWireToTerm(
	t testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	writeUnfragmentedWireToTermAt(t, termBuffer, 0, wire)
}

func appendUnfragmentedWireToTerm(
	t testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	writeUnfragmentedWireToTermAt(
		t,
		termBuffer,
		nextControlResponseHookTermWriteOffset(termBuffer),
		wire,
	)
}

func writeUnfragmentedWireToTermAt(
	t testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	offset int,
	wire []byte,
) {
	t.Helper()

	writeControlResponseHookCommittedFragmentFrames(
		t,
		termBuffer,
		offset,
		logbuffer.Unfragmented,
		wire,
	)
}

func writeTwoSequentialUnfragmentedWiresToTerm(
	t testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	first, second []byte,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(t, termBuffer, first)
	appendUnfragmentedWireToTerm(t, termBuffer, second)
}

func writeControlResponseCommittedFragmentFrame(
	t testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	t.Helper()

	writeHeader := logbuffer.WriteDataHeaderFragmented
	if flags&logbuffer.Unfragmented == logbuffer.Unfragmented {
		writeHeader = logbuffer.WriteDataHeaderUnfragmented
	}
	require.NoError(t, writeHeader(
		termBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
	))

	frameLength := int32(len(payload) + logbuffer.DataHeaderLength)
	termBuffer.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, frameLength)
}

const (
	controlResponseHookFrameSessionID  = int32(1002)
	controlResponseHookFrameStreamID   = int32(2003)
	controlResponseFragmentFrameTermID = int32(7)
)

type controlResponseFragmentFrameSpec struct {
	flags   uint8
	payload []byte
}

func splitControlResponseWireFragmentSpecs(wire []byte) []controlResponseFragmentFrameSpec {
	split := len(wire) / 2
	return []controlResponseFragmentFrameSpec{
		{flags: logbuffer.BeginFlag, payload: wire[:split]},
		{flags: logbuffer.EndFlag, payload: wire[split:]},
	}
}

func writeControlResponseHookCommittedFragmentFrames(
	t testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	startOffset int,
	flags uint8,
	payload []byte,
) {
	t.Helper()

	writeControlResponseHookCommittedFragmentFrameSpecs(
		t,
		termBuffer,
		startOffset,
		[]controlResponseFragmentFrameSpec{{flags: flags, payload: payload}},
	)
}

func writeControlResponseHookCommittedFragmentFrameSpecs(
	t testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	startOffset int,
	specs []controlResponseFragmentFrameSpec,
) {
	t.Helper()

	offset := startOffset
	for _, spec := range specs {
		writeControlResponseCommittedFragmentFrame(
			t,
			termBuffer,
			offset,
			controlResponseHookFrameSessionID,
			controlResponseHookFrameStreamID,
			controlResponseFragmentFrameTermID,
			int32(offset),
			spec.flags,
			spec.payload,
		)
		frameLength := logbuffer.FrameLengthVolatile(termBuffer, offset)
		offset += logbuffer.AlignFrameLength(int(frameLength))
	}
}

func writeFragmentedWireToTerm(
	t testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	writeControlResponseHookCommittedFragmentFrameSpecs(
		t,
		termBuffer,
		0,
		splitControlResponseWireFragmentSpecs(wire),
	)
}

func nextControlResponseHookTermWriteOffset(termBuffer atomicbuffer.AtomicBuffer) int {
	offset := 0
	for {
		frameLength := logbuffer.FrameLengthVolatile(termBuffer, offset)
		if frameLength <= 0 {
			return offset
		}
		next := offset + logbuffer.AlignFrameLength(int(frameLength))
		if next >= termBuffer.Capacity() {
			return offset
		}
		offset = next
	}
}

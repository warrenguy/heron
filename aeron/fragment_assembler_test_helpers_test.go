package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

type fragmentTestHelper interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

func deliverFragment(
	t fragmentTestHelper,
	assembler *FragmentAssembler,
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
) {
	t.Helper()

	frameLength := logbuffer.FrameLengthVolatile(termBuffer, frameOffset)
	payloadOffset := frameOffset + logbuffer.OffsetFramePayload
	payloadLength := int(frameLength) - logbuffer.DataHeaderLength
	header := &Header{}
	header.SetBuffer(termBuffer)
	header.SetOffset(frameOffset)
	header.ResetPollFragmentState()
	assembler.Handler()(termBuffer, payloadOffset, payloadLength, header)
}

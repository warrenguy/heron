package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// fragmentDeliveryArgs returns the handler arguments for the committed frame at
// frameOffset, as a poll would present them.
func fragmentDeliveryArgs(
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
) (payloadOffset, payloadLength int, header *Header) {
	frameLength := logbuffer.FrameLengthVolatile(termBuffer, frameOffset)
	header = &Header{}
	header.SetBuffer(termBuffer)
	header.SetOffset(frameOffset)
	header.ResetPollFragmentState()
	return frameOffset + logbuffer.OffsetFramePayload,
		int(frameLength) - logbuffer.DataHeaderLength,
		header
}

func deliverFragment(
	t fragmentTestHelper,
	assembler *FragmentAssembler,
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
) {
	t.Helper()

	payloadOffset, payloadLength, header := fragmentDeliveryArgs(termBuffer, frameOffset)
	assembler.Handler()(termBuffer, payloadOffset, payloadLength, header)
}

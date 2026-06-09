package aeron

import (
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

const headerFragmentedFrameNotSet int32 = -1

// Header is a flyweight reused per Image across Poll callbacks. SetBuffer and
// SetOffset mutate it each fragment; handlers must not retain *Header past return.
type Header struct {
	buffer              aeronatomic.AtomicBuffer
	offset              int
	initialTermID       int32
	positionBitsToShift int32
	context             any
	// fragmentedFrameLen -1 uses FrameLength for occupancy; non-negative overrides
	// termOccupancyLength for fragmented reassembly (unused until FragmentAssembler).
	fragmentedFrameLen int32
}

func (header *Header) FrameLength() int32 {
	return logbuffer.FrameLengthVolatile(header.buffer, header.offset)
}

func (header *Header) SessionID() int32 {
	return logbuffer.FrameSessionID(header.buffer, header.offset)
}

func (header *Header) StreamID() int32 {
	return logbuffer.FrameStreamID(header.buffer, header.offset)
}

func (header *Header) TermID() int32 {
	return logbuffer.FrameTermID(header.buffer, header.offset)
}

func (header *Header) TermOffset() int32 {
	return logbuffer.FrameTermOffset(header.buffer, header.offset)
}

func (header *Header) Type() int32 {
	return int32(logbuffer.FrameType(header.buffer, header.offset))
}

func (header *Header) Flags() byte {
	return logbuffer.FrameFlags(header.buffer, header.offset)
}

func (header *Header) ReservedValue() int64 {
	return logbuffer.FrameReservedValue(header.buffer, header.offset)
}

func (header *Header) Offset() int {
	return header.offset
}

func (header *Header) NextTermOffset() int32 {
	return int32(logbuffer.AlignFrameLength(int(header.TermOffset() + header.termOccupancyLength())))
}

func (header *Header) Position() int64 {
	return logbuffer.ComputePosition(
		header.TermID(),
		header.NextTermOffset(),
		int(header.positionBitsToShift),
		header.initialTermID,
	)
}

func (header *Header) SetOffset(offset int) {
	header.offset = offset
}

func (header *Header) SetBuffer(buffer aeronatomic.AtomicBuffer) {
	header.buffer = buffer
}

func (header *Header) termOccupancyLength() int32 {
	if header.fragmentedFrameLen == headerFragmentedFrameNotSet {
		return header.FrameLength()
	}
	return header.fragmentedFrameLen
}

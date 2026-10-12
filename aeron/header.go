package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

const headerFragmentedFrameNotSet int32 = -1

// Header is a flyweight reused per Image across Poll callbacks. SetBuffer and
// SetOffset mutate it each fragment.
//
// Handlers must not retain *Header past return. Handlers must not call
// ConfigureCompletedFragment on the Image.Poll flyweight; use the
// FragmentAssembler completedHeader only.
type Header struct {
	buffer              atomicbuffer.AtomicBuffer
	offset              int
	initialTermID       int32
	positionBitsToShift int
	context             any
	// fragmentedFrameLen: headerFragmentedFrameNotSet (-1) uses FrameLength for
	// term occupancy; non-negative overrides termOccupancyLength for a reassembled
	// fragmented message (set by ConfigureCompletedFragment, cleared in
	// invokeFragmentHandler before each delivered DATA FragmentHandler call).
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
	return logbuffer.AlignFrameLength(header.TermOffset() + header.termOccupancyLength())
}

func (header *Header) Position() int64 {
	return logbuffer.ComputePosition(
		header.TermID(),
		int(header.NextTermOffset()),
		header.positionBitsToShift,
		header.initialTermID,
	)
}

func (header *Header) SetOffset(offset int) {
	header.offset = offset
}

func (header *Header) SetBuffer(buffer atomicbuffer.AtomicBuffer) {
	header.buffer = buffer
}

// HeaderImageFields holds Image-level metadata on the poll flyweight (not in the
// 32-byte DATA frame header). Seeded by Image.Poll before FragmentHandler.
type HeaderImageFields struct {
	InitialTermID       int32
	PositionBitsToShift int
	Context             any
}

// ImageFields returns Image-level flyweight metadata for this header.
func (header *Header) ImageFields() HeaderImageFields {
	return HeaderImageFields{
		InitialTermID:       header.initialTermID,
		PositionBitsToShift: header.positionBitsToShift,
		Context:             header.context,
	}
}

// ResetPollFragmentState clears fragmented-frame override so NextTermOffset and
// term occupancy use the committed frame length. invokeFragmentHandler does this
// before each delivered DATA FragmentHandler call; manual test headers should call
// it for poll parity.
func (header *Header) ResetPollFragmentState() {
	header.fragmentedFrameLen = headerFragmentedFrameNotSet
}

// ConfigureCompletedFragment configures a dedicated reassembly header for a
// reassembled fragmented message. Mirrors Java BufferBuilder.completeHeader.
//
// Do not pass the Image.Poll flyweight as target or lastFragment from poll
// without an assembler-owned captured buffer.
//
// capturedFrameHeader is patched in place; retain it until the FragmentHandler returns.
// The slice must not alias a live term buffer; FragmentAssembler copies the BEGIN
// header into assembler-owned storage before calling this method. BEGIN frame length
// is read from capturedFrameHeader (committed FrameLength, not aligned term occupancy).
//
// Panics on nil lastFragment, short capturedFrameHeader, non-positive
// assembledPayloadLength, or captured BEGIN frame length not greater than DataHeaderLength.
func (header *Header) ConfigureCompletedFragment(
	capturedFrameHeader []byte,
	assembledPayloadLength int,
	lastFragment *Header,
) {
	if lastFragment == nil {
		panic("aeron: lastFragment is nil")
	}
	endFlags := lastFragment.Flags()
	if endFlags&logbuffer.EndFlag == 0 {
		panic("aeron: lastFragment missing EndFlag")
	}
	if len(capturedFrameHeader) < logbuffer.DataHeaderLength {
		panic("aeron: capturedFrameHeader too short")
	}
	if assembledPayloadLength <= 0 {
		panic("aeron: assembledPayloadLength must be positive")
	}
	if assembledPayloadLength > logbuffer.MaxMessageLength {
		panic("aeron: assembledPayloadLength exceeds MaxMessageLength")
	}
	dataHeaderLength := int32(logbuffer.DataHeaderLength)

	buf := atomicbuffer.Wrap(capturedFrameHeader)
	firstFrameLength := logbuffer.FrameLength(buf, 0)
	if firstFrameLength <= dataHeaderLength {
		panic("aeron: firstFrameLength must exceed data header length")
	}

	logbuffer.PutFrameLengthOrdered(
		buf,
		0,
		dataHeaderLength+int32(assembledPayloadLength),
	)
	logbuffer.PutFrameFlags(
		buf,
		0,
		logbuffer.FrameFlags(buf, 0)|endFlags,
	)

	maxPayloadLength := int(firstFrameLength - dataHeaderLength)
	//nolint:gosec // G115: fits in int32 for any payload up to MaxMessageLength
	header.fragmentedFrameLen = int32(logbuffer.ComputeFragmentedFrameLength(
		assembledPayloadLength,
		maxPayloadLength,
	))
	header.SetBuffer(buf)
	header.SetOffset(0)
	header.initialTermID = lastFragment.initialTermID
	header.positionBitsToShift = lastFragment.positionBitsToShift
	header.context = lastFragment.context
}

func (header *Header) termOccupancyLength() int32 {
	if header.fragmentedFrameLen == headerFragmentedFrameNotSet {
		return header.FrameLength()
	}
	return header.fragmentedFrameLen
}

package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

const fragmentBuilderIdleNextTermOffset int32 = -1

const fragmentBuilderInitMinCapacity = 4096

// FragmentAssembler reassembles B|E fragmented DATA frames and forwards whole
// payloads to the delegate FragmentHandler. Unfragmented frames delegate without
// copy. Reassembled delegates receive an assembler-owned buffer slice and
// &builder.completedHeader; both are valid only for the delegate callback and
// must not be retained past return. Not safe for concurrent use or reentrant
// delegate callbacks that poll the same assembler.
type FragmentAssembler struct {
	delegate            aeron.FragmentHandler
	directAssemblers    bool
	initialBufferLength int
	builders            map[int32]*fragmentBufferBuilder
}

type fragmentBufferBuilder struct {
	payload             []byte
	limit               int
	nextTermOffset      int32
	capturedFrameHeader [logbuffer.DataHeaderLength]byte
	completedHeader     aeron.Header
}

// NewFragmentAssembler returns a FragmentAssembler with default options.
func NewFragmentAssembler(delegate aeron.FragmentHandler) *FragmentAssembler {
	return NewFragmentAssemblerWithOptions(delegate, 0, false)
}

// NewFragmentAssemblerWithOptions constructs a FragmentAssembler.
//
// directAssemblers is stored for Java API parity; reassembly uses heap buffers only.
// initialBufferLength is clamped: negative values become 0; values above
// logbuffer.MaxMessageLength become logbuffer.MaxMessageLength.
func NewFragmentAssemblerWithOptions(
	delegate aeron.FragmentHandler,
	initialBufferLength int,
	directAssemblers bool,
) *FragmentAssembler {
	if initialBufferLength < 0 {
		initialBufferLength = 0
	}
	if initialBufferLength > logbuffer.MaxMessageLength {
		initialBufferLength = logbuffer.MaxMessageLength
	}
	return &FragmentAssembler{
		delegate:            delegate,
		directAssemblers:    directAssemblers,
		initialBufferLength: initialBufferLength,
		builders:            make(map[int32]*fragmentBufferBuilder),
	}
}

// Handler returns the FragmentHandler passed to Subscription.Poll.
func (assembler *FragmentAssembler) Handler() aeron.FragmentHandler {
	return assembler.handleFragment
}

// DirectAssemblers reports the isDirectAssemblers policy. Heap buffers only until
// a future direct-backing optimization lands.
func (assembler *FragmentAssembler) DirectAssemblers() bool {
	return assembler.directAssemblers
}

// Clear drops all in-progress reassembly state.
func (assembler *FragmentAssembler) Clear() {
	clear(assembler.builders)
}

// FreeSessionBuffer evicts the reassembly builder for sessionID.
func (assembler *FragmentAssembler) FreeSessionBuffer(sessionID int32) bool {
	_, ok := assembler.builders[sessionID]
	delete(assembler.builders, sessionID)
	return ok
}

func (assembler *FragmentAssembler) handleFragment(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	flags := header.Flags()
	if flags&logbuffer.Unfragmented == logbuffer.Unfragmented {
		assembler.delegate(buffer, offset, length, header)
		return
	}

	sessionID := header.SessionID()
	builder := assembler.builders[sessionID]

	switch {
	case flags&logbuffer.BeginFlag != 0:
		if builder == nil {
			builder = &fragmentBufferBuilder{}
			assembler.builders[sessionID] = builder
		}
		builder.reset()
		builder.captureHeader(header, buffer)
		if !builder.append(buffer, offset, length, assembler.initialBufferLength) {
			return
		}
		builder.nextTermOffset = header.NextTermOffset()

	default:
		if builder == nil {
			return
		}
		if !builder.appendAtExpectedOffset(
			buffer,
			offset,
			length,
			header.TermOffset(),
			assembler.initialBufferLength,
		) {
			return
		}
		if flags&logbuffer.EndFlag != 0 {
			firstLen := builder.capturedFirstFrameLength()
			if builder.limit <= 0 || firstLen <= int32(logbuffer.DataHeaderLength) {
				builder.reset()
				return
			}
			// ConfigureCompletedFragment reads Image flyweight metadata from lastFragment
			// (END header); all fragments share the same Image so END carries correct
			// initialTermID/positionBitsToShift/context.
			builder.completedHeader.ConfigureCompletedFragment(
				builder.capturedFrameHeader[:],
				builder.limit,
				header,
			)
			delete(assembler.builders, sessionID)
			assembler.delegate(
				atomic.Wrap(builder.payload),
				0,
				builder.limit,
				&builder.completedHeader,
			)
			return
		}
		builder.nextTermOffset = header.NextTermOffset()
	}
}

func (builder *fragmentBufferBuilder) reset() {
	builder.limit = 0
	builder.nextTermOffset = fragmentBuilderIdleNextTermOffset
}

func (builder *fragmentBufferBuilder) capturedFirstFrameLength() int32 {
	return logbuffer.FrameLength(atomic.Wrap(builder.capturedFrameHeader[:]), 0)
}

func (builder *fragmentBufferBuilder) captureHeader(
	header *aeron.Header,
	buffer atomic.AtomicBuffer,
) {
	frameOffset := header.Offset()
	copy(
		builder.capturedFrameHeader[:],
		buffer.Bytes()[frameOffset:frameOffset+logbuffer.DataHeaderLength],
	)
}

func (builder *fragmentBufferBuilder) appendAtExpectedOffset(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	termOffset int32,
	initialBufferLength int,
) bool {
	if termOffset != builder.nextTermOffset {
		builder.reset()
		return false
	}
	return builder.append(buffer, offset, length, initialBufferLength)
}

func (builder *fragmentBufferBuilder) append(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	initialBufferLength int,
) bool {
	bytes := buffer.Bytes()
	if length < 0 || offset < 0 || offset+length > len(bytes) {
		builder.reset()
		return false
	}
	if !builder.ensureCapacity(length, initialBufferLength) {
		builder.reset()
		return false
	}
	copy(builder.payload[builder.limit:builder.limit+length], bytes[offset:offset+length])
	builder.limit += length
	return true
}

func (builder *fragmentBufferBuilder) ensureCapacity(
	additional int,
	initialBufferLength int,
) bool {
	required := builder.limit + additional
	if required > logbuffer.MaxMessageLength {
		return false
	}
	if required <= cap(builder.payload) {
		return true
	}

	next := cap(builder.payload)
	if next == 0 {
		next = max(initialBufferLength, fragmentBuilderInitMinCapacity)
	}
	for next < required {
		next *= 2
		if next > logbuffer.MaxMessageLength {
			next = logbuffer.MaxMessageLength
			break
		}
	}
	grown := make([]byte, next)
	copy(grown, builder.payload[:builder.limit])
	builder.payload = grown
	return true
}

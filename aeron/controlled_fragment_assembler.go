package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// ControlledFragmentAssembler reassembles BEGIN/middle/END fragmented DATA
// frames and forwards whole payloads to the delegate ControlledFragmentHandler.
//
// Unfragmented frames delegate without copy. The delegate action returns to
// the poll loop.
//
// BEGIN and middle fragments append to a per-session builder. Those paths
// return [ControlledContinue] without calling the delegate.
//
// END appends, then delegates the assembled buffer and completed header.
// Delegate [ControlledAbort] on END rewinds the builder limit to the pre-END
// length. The builder stays so a redelivered END re-appends and re-delivers.
// Any other action drops the builder.
//
// Reassembled delegates receive an assembler-owned buffer slice and
// &builder.completedHeader. Both are valid only for the delegate callback.
// Callers must not retain either past return.
//
// Not safe for concurrent use. Not safe for reentrant delegate callbacks that
// poll the same assembler.
type ControlledFragmentAssembler struct {
	delegate            ControlledFragmentHandler
	directAssemblers    bool
	initialBufferLength int
	builders            map[int32]*fragmentBufferBuilder
}

// NewControlledFragmentAssembler returns a ControlledFragmentAssembler with
// default options.
func NewControlledFragmentAssembler(
	delegate ControlledFragmentHandler,
) *ControlledFragmentAssembler {
	return NewControlledFragmentAssemblerWithOptions(delegate, 0, false)
}

// NewControlledFragmentAssemblerWithOptions constructs a ControlledFragmentAssembler.
//
// directAssemblers is stored for Java API parity; reassembly uses heap buffers only.
// initialBufferLength is clamped: negative values become 0; values above
// logbuffer.MaxMessageLength become logbuffer.MaxMessageLength.
func NewControlledFragmentAssemblerWithOptions(
	delegate ControlledFragmentHandler,
	initialBufferLength int,
	directAssemblers bool,
) *ControlledFragmentAssembler {
	initialBufferLength = max(0, min(initialBufferLength, logbuffer.MaxMessageLength))
	return &ControlledFragmentAssembler{
		delegate:            delegate,
		directAssemblers:    directAssemblers,
		initialBufferLength: initialBufferLength,
		builders:            make(map[int32]*fragmentBufferBuilder),
	}
}

// Handler returns the ControlledFragmentHandler for
// [Subscription.ControlledPoll] or [Image.ControlledPoll].
//
// Allocates a method value per call. Duty-cycle callers must bind the result
// once and reuse it. Do not call Handler on every poll.
func (assembler *ControlledFragmentAssembler) Handler() ControlledFragmentHandler {
	return assembler.handleFragment
}

// DirectAssemblers reports the isDirectAssemblers policy. Heap buffers only until
// a future direct-backing optimization lands.
func (assembler *ControlledFragmentAssembler) DirectAssemblers() bool {
	return assembler.directAssemblers
}

// Clear drops all in-progress reassembly state.
func (assembler *ControlledFragmentAssembler) Clear() {
	clear(assembler.builders)
}

// FreeSessionBuffer evicts the reassembly builder for sessionID.
func (assembler *ControlledFragmentAssembler) FreeSessionBuffer(sessionID int32) bool {
	_, ok := assembler.builders[sessionID]
	delete(assembler.builders, sessionID)
	return ok
}

func (assembler *ControlledFragmentAssembler) handleFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *Header,
) ControlledAction {
	flags := header.Flags()
	if flags&logbuffer.Unfragmented == logbuffer.Unfragmented {
		return assembler.delegate(buffer, offset, length, header)
	}

	sessionID := header.SessionID()
	builder := assembler.builders[sessionID]

	if flags&logbuffer.BeginFlag != 0 {
		if builder == nil {
			builder = &fragmentBufferBuilder{}
			assembler.builders[sessionID] = builder
		}
		builder.reset()
		builder.captureHeader(header, buffer)
		if !builder.append(buffer, offset, length, assembler.initialBufferLength) {
			return ControlledContinue
		}
	} else {
		if builder == nil {
			return ControlledContinue
		}

		// Save limit before append so END+Abort can rewind without re-copying
		// prior fragments. Redelivered END re-appends from this limit.
		preEndLimit := builder.limit
		if !builder.appendAtExpectedOffset(
			buffer,
			offset,
			length,
			header.TermOffset(),
			assembler.initialBufferLength,
		) {
			return ControlledContinue
		}

		if flags&logbuffer.EndFlag != 0 {
			// ConfigureCompletedFragment patches frame length in place. Restore the
			// BEGIN first-frame length first so Abort redelivery recomputes span and
			// Position from the original max payload length (Java BufferBuilder
			// firstFrameLength).
			builder.restoreFirstFrameLength()
			if builder.limit <= 0 || builder.firstFrameLength <= int32(logbuffer.DataHeaderLength) {
				// Malformed END is terminal: no valid payload can be delivered.
				// Evict the idle map entry. Transient failures (offset gap, append
				// bounds) only reset the builder so a later BEGIN can reuse the slot.
				delete(assembler.builders, sessionID)
				return ControlledContinue
			}

			builder.completedHeader.ConfigureCompletedFragment(
				builder.capturedFrameHeader[:],
				builder.limit,
				header,
			)
			action := assembler.delegate(
				atomicbuffer.Wrap(builder.payload),
				0,
				builder.limit,
				&builder.completedHeader,
			)
			if action == ControlledAbort {
				// Abort rewinds the payload limit only. The redelivered END path
				// restores first-frame length before ConfigureCompletedFragment.
				builder.limit = preEndLimit
				return ControlledAbort
			}
			delete(assembler.builders, sessionID)
			return action
		}
	}
	builder.nextTermOffset = header.NextTermOffset()
	return ControlledContinue
}

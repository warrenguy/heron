package aeron

import (
	"bytes"

	"github.com/warrenguy/heron/atomicbuffer"
)

// FragmentHandlerBytes receives one committed fragment payload during Poll.
//
// payload contains application bytes only (no frame header).
//
// AdaptFragmentHandlerBytes slices the term buffer at the poll-provided offset and
// length from FragmentHandler (payload start, past the 32-byte frame header).
//
// payload is a zero-copy view into the term buffer when delivered via
// AdaptFragmentHandlerBytes. It is valid only for the callback duration unless
// copied. AdaptFragmentHandlerBytesRetain delivers a bytes.Clone copy
// retain-safe after return; payload lifetime then matches the clone, not the
// term buffer. payload is nil when length <= 0 or the span is out of bounds.
// The adapter invokes the handler even when payload is nil (e.g. zero-length
// DATA frames).
//
// Handlers must not retain the Header pointer beyond the callback; copy fields
// if needed after return. See FragmentHandler for the full poll contract.
type FragmentHandlerBytes func(payload []byte, header *Header)

// AdaptFragmentHandlerBytes wraps h as a FragmentHandler suitable for
// Subscription.Poll, Image.Poll, or FragmentAssembler delegate wiring.
//
// Returns nil when h is nil (Poll treats nil handler as no-op).
//
// Cold-path ergonomics: may allocate the adapted closure once. Per-fragment
// delivery is zero-alloc (slice view only). Use AdaptFragmentHandlerBytesRetain
// when bytes.Clone per fragment is required for retain-after-return.
//
// Example:
//
//	sub.Poll(aeron.AdaptFragmentHandlerBytes(func(p []byte, h *aeron.Header) {
//	    _ = p // zero-copy; valid only for callback
//	}), 10)
func AdaptFragmentHandlerBytes(h FragmentHandlerBytes) FragmentHandler {
	return adaptFragmentHandlerBytes(h, fragmentPayloadView)
}

// AdaptFragmentHandlerBytesRetain wraps h as a FragmentHandler that passes a
// bytes.Clone copy of the payload retain-safe after return. header lifetime
// rules are unchanged (callback-scoped; do not retain *Header).
//
// Returns nil when h is nil.
//
// Allocates a bytes.Clone copy per fragment.
//
// Prefer AdaptFragmentHandlerBytes for in-callback decode on hot paths; use this
// adapter when storing payload across poll spins.
func AdaptFragmentHandlerBytesRetain(h FragmentHandlerBytes) FragmentHandler {
	return adaptFragmentHandlerBytes(h, fragmentPayloadClone)
}

// ControlledFragmentHandlerBytes receives one committed fragment payload during
// ControlledPoll and returns the action for the poll loop.
//
// payload follows the FragmentHandlerBytes span rules. The span is a zero-copy
// view valid only for the callback duration. payload is nil when length <= 0 or
// the span is out of bounds. The adapter still invokes the handler when payload
// is nil.
//
// Handlers must not retain payload or the Header pointer beyond the callback.
// See ControlledFragmentHandler for the full poll contract.
type ControlledFragmentHandlerBytes func(payload []byte, header *Header) ControlledAction

// AdaptControlledFragmentHandlerBytes wraps h as a ControlledFragmentHandler for
// Subscription.ControlledPoll, Image.ControlledPoll, or
// ControlledFragmentAssembler delegate wiring.
//
// Returns nil when h is nil. ControlledPoll treats a nil handler as a no-op.
//
// May allocate the adapted closure once on the cold path. Per-fragment delivery
// is zero-alloc (slice view only).
func AdaptControlledFragmentHandlerBytes(
	h ControlledFragmentHandlerBytes,
) ControlledFragmentHandler {
	if h == nil {
		return nil
	}
	return func(
		buffer atomicbuffer.AtomicBuffer,
		offset, length int,
		header *Header,
	) ControlledAction {
		return h(fragmentPayloadView(buffer, offset, length), header)
	}
}

func adaptFragmentHandlerBytes(
	h FragmentHandlerBytes,
	payload func(atomicbuffer.AtomicBuffer, int, int) []byte,
) FragmentHandler {
	if h == nil {
		return nil
	}
	return func(buffer atomicbuffer.AtomicBuffer, offset, length int, header *Header) {
		h(payload(buffer, offset, length), header)
	}
}

// fragmentPayloadView mirrors cluster/client/egress_handler.go:cloneAtomicBufferSpan
// bounds rules. Invalid spans yield nil without panic; do not use
// atomicbuffer.AtomicBuffer.Slice, which panics on bounds violation.
func fragmentPayloadView(buffer atomicbuffer.AtomicBuffer, offset, length int) []byte {
	if length <= 0 {
		return nil
	}
	raw := buffer.Bytes()
	if offset < 0 || offset > len(raw)-length {
		return nil
	}
	return raw[offset : offset+length]
}

func fragmentPayloadClone(buffer atomicbuffer.AtomicBuffer, offset, length int) []byte {
	return bytes.Clone(fragmentPayloadView(buffer, offset, length))
}

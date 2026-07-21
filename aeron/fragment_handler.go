package aeron

import (
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/ringbuffer"
)

// FragmentHandler receives one committed fragment during [Poll]. Offset points past
// the 32-byte frame header at payload start.
//
// The buffer view is valid only for the callback duration.
//
// Handlers must not retain the [Header] pointer beyond the callback; copy fields if
// needed after return.
type FragmentHandler func(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *Header,
)

// ControlledAction is [ringbuffer.ControlledAction] for Image controlled poll.
type ControlledAction = ringbuffer.ControlledAction

const (
	ControlledAbort    = ringbuffer.ControlledAbort
	ControlledBreak    = ringbuffer.ControlledBreak
	ControlledCommit   = ringbuffer.ControlledCommit
	ControlledContinue = ringbuffer.ControlledContinue
)

// ControlledFragmentHandler receives one committed DATA fragment during
// [ControlledPoll] or [BoundedControlledPoll] and returns the position action.
//
// Offset points past the 32-byte frame header at payload start.
// Buffer and [Header] are valid only for the callback duration.
//
// Handlers must not retain the [Header] pointer beyond the callback; copy fields if
// needed after return.
//
// Action values are [ControlledAction] ([ControlledAbort], [ControlledBreak],
// [ControlledCommit], [ControlledContinue]).
type ControlledFragmentHandler func(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *Header,
) ControlledAction

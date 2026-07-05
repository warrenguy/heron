package client

import (
	"github.com/warrenguy/heron/aeron"
)

// FragmentAssembler reassembles BEGIN/middle/END fragmented DATA frames and forwards whole
// payloads to the delegate FragmentHandler. Unfragmented frames delegate without
// copy. Reassembled delegates receive an assembler-owned buffer slice and
// &builder.completedHeader; both are valid only for the delegate callback and
// must not be retained past return. Not safe for concurrent use or reentrant
// delegate callbacks that poll the same assembler.
type FragmentAssembler struct {
	core *aeron.FragmentAssembler

	// demuxTargets lets PollArchiveControlEvents route reassembled fragments by
	// value, avoiding a per-poll delegate closure so the demux hot path stays
	// allocation-free.
	demuxTargets ArchiveControlPollTargets
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
	assembler := &FragmentAssembler{}
	handler := delegate
	if handler == nil {
		handler = assembler.dispatchArchiveControlFragment
	}
	assembler.core = aeron.NewFragmentAssemblerWithOptions(
		handler,
		initialBufferLength,
		directAssemblers,
	)
	return assembler
}

// Handler returns the FragmentHandler passed to Subscription.Poll.
func (assembler *FragmentAssembler) Handler() aeron.FragmentHandler {
	return assembler.core.Handler()
}

// DirectAssemblers reports the isDirectAssemblers policy. Heap buffers only until
// a future direct-backing optimization lands.
func (assembler *FragmentAssembler) DirectAssemblers() bool {
	return assembler.core.DirectAssemblers()
}

// Clear drops all in-progress reassembly state.
func (assembler *FragmentAssembler) Clear() {
	assembler.core.Clear()
}

// FreeSessionBuffer evicts the reassembly builder for sessionID.
func (assembler *FragmentAssembler) FreeSessionBuffer(sessionID int32) bool {
	return assembler.core.FreeSessionBuffer(sessionID)
}

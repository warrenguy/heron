package archive

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
)

// FragmentAssembler reassembles BEGIN/middle/END fragmented DATA frames into
// whole payloads.
//
// Behaviour: a non-nil delegate receives each whole payload. A nil delegate
// selects the demux form: handleControlFragment receives each whole payload
// and returns the poll action. PollControlEvents drives the demux form.
// Unfragmented frames reach the delegate without a copy.
//
// Lifetime: a reassembled payload arrives in an assembler-owned buffer with an
// assembler-owned header. Both are valid only during the delegate call. Each
// session keeps its builder, and its payload capacity, after delivery until
// FreeSessionBuffer or Clear evicts it.
//
// Concurrency: not safe for concurrent use. The delegate must not poll the
// same assembler.
type FragmentAssembler struct {
	core     *aeron.ControlledFragmentAssembler
	delegate aeron.FragmentHandler

	// controlledHandler caches core.Handler(). Each call allocates a method
	// value.
	controlledHandler aeron.ControlledFragmentHandler

	// demuxTargets holds the poll targets by value. PollControlEvents sets it
	// each poll. A per-poll closure would allocate on the demux hot path.
	demuxTargets ControlPollTargets

	// errorResponse holds one routed ERROR for the current PollControlEvents
	// call.
	errorResponse error
}

// NewFragmentAssembler returns a FragmentAssembler with default options.
func NewFragmentAssembler(delegate aeron.FragmentHandler) *FragmentAssembler {
	return NewFragmentAssemblerWithOptions(delegate, 0, false)
}

// NewFragmentAssemblerWithOptions constructs a FragmentAssembler. A nil
// delegate selects the demux form for PollControlEvents.
//
// directAssemblers exists for Java API parity. Reassembly uses heap buffers
// only. initialBufferLength clamps to 0 through logbuffer.MaxMessageLength.
func NewFragmentAssemblerWithOptions(
	delegate aeron.FragmentHandler,
	initialBufferLength int,
	directAssemblers bool,
) *FragmentAssembler {
	assembler := &FragmentAssembler{delegate: delegate}
	wholeFragmentHandler := assembler.handleDemuxFragment
	if delegate != nil {
		wholeFragmentHandler = assembler.handleDelegateFragment
	}
	assembler.core = aeron.NewControlledFragmentAssemblerWithOptions(
		wholeFragmentHandler,
		initialBufferLength,
		directAssemblers,
	)
	assembler.controlledHandler = assembler.core.Handler()
	return assembler
}

// Handler returns a FragmentHandler for Subscription.Poll.
//
// A plain poll drops the poll action. Poll the demux form with
// PollControlEvents.
func (assembler *FragmentAssembler) Handler() aeron.FragmentHandler {
	return assembler.handleFragment
}

// DirectAssemblers reports the isDirectAssemblers policy. Heap buffers only until
// a future direct-backing optimization lands.
func (assembler *FragmentAssembler) DirectAssemblers() bool {
	return assembler.core.DirectAssemblers()
}

// Clear drops every reassembly builder, in progress or idle.
func (assembler *FragmentAssembler) Clear() {
	assembler.core.Clear()
}

// FreeSessionBuffer evicts the reassembly builder for sessionID.
func (assembler *FragmentAssembler) FreeSessionBuffer(sessionID int32) bool {
	return assembler.core.FreeSessionBuffer(sessionID)
}

func (assembler *FragmentAssembler) handleFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	assembler.controlledHandler(buffer, offset, length, header)
}

func (assembler *FragmentAssembler) handleDelegateFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) aeron.ControlledAction {
	assembler.delegate(buffer, offset, length, header)
	return aeron.ControlledContinue
}

func (assembler *FragmentAssembler) takeErrorResponse() error {
	err := assembler.errorResponse
	assembler.errorResponse = nil
	return err
}

func (assembler *FragmentAssembler) handleDemuxFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) aeron.ControlledAction {
	return handleControlFragment(
		assembler.demuxTargets,
		&assembler.errorResponse,
		buffer,
		offset,
		length,
		header,
	)
}

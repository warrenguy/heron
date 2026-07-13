package archive

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

// ControlPollTargets routes archive control fragments during a demux poll.
//
// SignalHandler receives RecordingSignalEvent dispatches; ControlSessionID filters
// them (0 = all sessions).
//
// ResponsePoller captures ControlResponse and Challenge terminals. The demux never
// resets it: once it latches a terminal capture, handleControlResponseFromFragment drops later
// responses, and the poller's own Poll reset is bypassed here. Reuse across separate
// awaits requires an unlatched poller: call ResetCapture before each new demux await, or
// supply a fresh poller.
//
// DescriptorPoller captures RecordingDescriptor fragments. In descriptor mode
// (ResponsePoller nil, DescriptorPoller set) ControlResponse also routes to the
// descriptor poller so a list-recording await does not corrupt the session
// ResponsePoller. Steady-state duty may set both pollers: ControlResponse routes to
// ResponsePoller and descriptors route to the idle DescriptorPoller.
//
// A nil target drops its templates.
type ControlPollTargets struct {
	SignalHandler    RecordingSignalHandler
	ControlSessionID int64
	ResponsePoller   *ControlResponsePoller
	DescriptorPoller *RecordingDescriptorPoller
}

// PollControlEvents polls subscription once and demultiplexes archive control
// fragments to targets by SBE template id, reassembling fragmented messages through
// assembler. It never calls DoWork.
//
// Recording signals and control responses share one subscription; this demux is the
// only correct poll for a post-connect session. A standalone ControlResponsePoller.Poll
// or an isolated signal poll on the same subscription loses fragments.
//
// assembler must be non-nil and dedicated to the demux: use NewFragmentAssembler(nil)
// so the reassembly delegate routes through dispatchControlFragment. Targets
// are stored by value each poll.
//
// The demux does not reset targets.ResponsePoller; a reused poller that already latched
// a terminal drops later responses. See ControlPollTargets.ResponsePoller.
//
// Returns 0 when subscription is nil.
func PollControlEvents(
	subscription *aeron.Subscription,
	fragmentLimit int,
	assembler *FragmentAssembler,
	targets ControlPollTargets,
) int {
	if subscription == nil {
		return 0
	}
	assembler.demuxTargets = targets
	return subscription.Poll(assembler.Handler(), fragmentLimit)
}

func (assembler *FragmentAssembler) dispatchControlFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	handleControlFragment(assembler.demuxTargets, buffer, offset, length, header)
}

// handleControlFragment routes one reassembled control fragment by template id.
// Unknown or unroutable templates are ignored. Downstream typed decode validates schema
// and bounds, so a foreign-schema fragment routed by template id is dropped there.
func handleControlFragment(
	targets ControlPollTargets,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	fragment, ok := controlResponseFragmentPayload(buffer, offset, length)
	if !ok {
		return
	}
	// controlResponseFragmentPayload already guarantees len(fragment) >= header size, so
	// this decode cannot fail; the guard is defensive against future payload changes.
	_, templateID, _, _, ok := codecs.DecodeMessageHeader(fragment, 0)
	if !ok {
		return
	}
	switch templateID {
	case archivewire.RecordingSignalEventSbeTemplateID:
		handleRecordingSignalFromFragment(
			targets.SignalHandler,
			targets.ControlSessionID,
			fragment,
		)

	case archivewire.ControlResponseSbeTemplateID, archivewire.ChallengeSbeTemplateID:
		if targets.ResponsePoller != nil {
			handleControlResponseFromFragment(targets.ResponsePoller, fragment, templateID)
		} else if templateID == archivewire.ControlResponseSbeTemplateID &&
			targets.DescriptorPoller != nil {
			handleDescriptorControlResponseFragment(targets.DescriptorPoller, fragment)
		}

	case archivewire.RecordingDescriptorSbeTemplateID:
		if targets.DescriptorPoller != nil {
			handleRecordingDescriptorFragment(targets.DescriptorPoller, fragment)
		}
	}
}

package archive

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

// ControlPollTargets routes archive control fragments during a demux poll.
//
// SignalHandler receives RecordingSignalEvent dispatches; ControlSessionID filters
// them (0 = all sessions).
//
// ResponsePoller latches ControlResponse and Challenge terminals. While it
// holds a terminal, the demux stops the poll at the next response the
// correlation filter accepts. That frame and every frame behind it wait in the
// image for a later poll. See ControlResponsePoller.
//
// DescriptorPoller captures RecordingDescriptor fragments. With ResponsePoller
// nil, ControlResponse also routes to DescriptorPoller, so a list-recording
// await does not corrupt the session poller. DescriptorPoller latches only the
// correlation id NewAsyncListRecording armed.
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
// assembler must be non-nil and built with NewFragmentAssembler(nil). A
// non-nil delegate takes every fragment and nothing reaches targets.
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
	targets.ResponsePoller.clearDisarmedCapture()
	assembler.demuxTargets = targets
	return subscription.ControlledPoll(assembler.controlledHandler, fragmentLimit)
}

// handleControlFragment routes one reassembled control fragment by template id.
// Unknown and unroutable templates drop. The typed decode downstream validates
// schema and bounds, so a foreign-schema fragment drops there.
//
// Returns ControlledAbort when targets.ResponsePoller holds a latched terminal
// and the correlation filter accepts the fragment. Returns ControlledContinue
// otherwise.
func handleControlFragment(
	targets ControlPollTargets,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) aeron.ControlledAction {
	fragment, templateID, ok := controlFragmentTemplate(buffer, offset, length)
	if !ok {
		return aeron.ControlledContinue
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
			return handleControlResponseFromFragment(targets.ResponsePoller, fragment, templateID)
		}
		if templateID == archivewire.ControlResponseSbeTemplateID &&
			targets.DescriptorPoller != nil {
			handleDescriptorControlResponseFragment(targets.DescriptorPoller, fragment)
		}

	case archivewire.RecordingDescriptorSbeTemplateID:
		if targets.DescriptorPoller != nil {
			handleRecordingDescriptorFragment(targets.DescriptorPoller, fragment)
		}
	}
	return aeron.ControlledContinue
}

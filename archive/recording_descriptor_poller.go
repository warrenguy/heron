package archive

import (
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

// RecordingDescriptorPoller holds list-recording capture state for the archive control
// demux. It owns no subscription and no FragmentAssembler and exposes no standalone
// Poll: PollControlEvents drives all capture.
//
// A list recording completes when a matching RecordingDescriptor is captured, or when a
// non-OK ControlResponse arrives. Java Aeron Archive list-by-id may send only the
// descriptor; other flows may send an OK ControlResponse before the descriptor. The demux
// routes both templates to this poller in descriptor mode. Call ResetAwait before each
// request.
//
// Captured descriptor var-ascii fields are cloned at latch time for retention across
// later polls.
//
// Not safe for concurrent use.
type RecordingDescriptorPoller struct {
	complete           bool
	awaitCorrelationID int64
	descriptor         codecs.RecordingDescriptorFields
	response           codecs.ControlResponseFields
	hasResponse        bool
}

// NewRecordingDescriptorPoller returns an idle descriptor poller. Call ResetAwait before
// the first list-recording request.
func NewRecordingDescriptorPoller() *RecordingDescriptorPoller {
	return &RecordingDescriptorPoller{}
}

// ResetAwait clears prior capture and arms the poller for correlationID.
func (poller *RecordingDescriptorPoller) ResetAwait(correlationID int64) {
	poller.complete = false
	poller.awaitCorrelationID = correlationID
	poller.descriptor = codecs.RecordingDescriptorFields{}
	poller.response = codecs.ControlResponseFields{}
	poller.hasResponse = false
}

// disarm clears the capture and the filter when the filter still holds
// correlationID. A disarmed poller matches nothing.
func (poller *RecordingDescriptorPoller) disarm(correlationID int64) {
	if poller == nil || poller.awaitCorrelationID != correlationID {
		return
	}
	poller.ResetAwait(conductor.NullCorrelationID)
}

// IsPollComplete reports whether a matching RecordingDescriptor was captured, or a
// non-OK ControlResponse latched terminal completion.
func (poller *RecordingDescriptorPoller) IsPollComplete() bool {
	return poller.complete
}

// Descriptor returns the captured RecordingDescriptor fields with var-ascii bytes
// cloned at latch time.
func (poller *RecordingDescriptorPoller) Descriptor() codecs.RecordingDescriptorFields {
	return poller.descriptor
}

// ControlResponse returns the captured ControlResponse fields and whether one was
// captured for the awaited correlation id.
func (poller *RecordingDescriptorPoller) ControlResponse() (codecs.ControlResponseFields, bool) {
	if !poller.hasResponse {
		return codecs.ControlResponseFields{}, false
	}
	return poller.response, true
}

func (poller *RecordingDescriptorPoller) latchResponse(resp codecs.ControlResponseFields) {
	poller.response = resp
	poller.hasResponse = true
	if resp.Code != archivewire.ControlResponseCode_OK {
		poller.complete = true
	}
}

func (poller *RecordingDescriptorPoller) latchDescriptor(
	fields codecs.RecordingDescriptorFields,
) {
	poller.descriptor = codecs.CloneRecordingDescriptorFields(fields)
	// Java list-by-id may send RecordingDescriptor without a preceding OK response;
	// a matching descriptor always completes the await.
	poller.complete = true
}

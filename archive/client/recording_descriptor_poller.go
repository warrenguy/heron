package client

import (
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

type listRecordingAwaitPhase uint8

const (
	listRecordingAwaitingResponse listRecordingAwaitPhase = iota
	listRecordingAwaitingDescriptor
	listRecordingComplete
)

// RecordingDescriptorPoller holds list-recording capture state for the archive control
// demux. It owns no subscription and no FragmentAssembler and exposes no standalone
// Poll: PollArchiveControlEvents drives all capture.
//
// A list recording completes in two phases on one subscription: an OK ControlResponse
// followed by the RecordingDescriptor carrying the same correlation id. The demux routes
// both to this poller in descriptor mode. Call ResetAwait before each request.
//
// Captured descriptor var-ascii fields are cloned at latch time for retention across
// later polls.
//
// Not safe for concurrent use.
type RecordingDescriptorPoller struct {
	phase              listRecordingAwaitPhase
	awaitCorrelationID int64
	descriptor         codecs.RecordingDescriptorFields
	response           codecs.ControlResponseFields
	hasDescriptor      bool
}

// NewRecordingDescriptorPoller returns an idle descriptor poller. Call ResetAwait before
// the first list-recording request.
func NewRecordingDescriptorPoller() *RecordingDescriptorPoller {
	return &RecordingDescriptorPoller{}
}

// ResetAwait clears prior capture and arms the poller for correlationID.
func (poller *RecordingDescriptorPoller) ResetAwait(correlationID int64) {
	poller.phase = listRecordingAwaitingResponse
	poller.awaitCorrelationID = correlationID
	poller.descriptor = codecs.RecordingDescriptorFields{}
	poller.response = codecs.ControlResponseFields{}
	poller.hasDescriptor = false
}

// IsPollComplete reports whether an OK ControlResponse and the matching
// RecordingDescriptor were captured, or a non-OK response latched terminal completion.
func (poller *RecordingDescriptorPoller) IsPollComplete() bool {
	return poller.phase == listRecordingComplete
}

// Descriptor returns the captured RecordingDescriptor fields with var-ascii bytes
// cloned at latch time.
func (poller *RecordingDescriptorPoller) Descriptor() codecs.RecordingDescriptorFields {
	return poller.descriptor
}

// ControlResponse returns the captured ControlResponse fields and whether one was
// captured for the awaited correlation id.
func (poller *RecordingDescriptorPoller) ControlResponse() (codecs.ControlResponseFields, bool) {
	if poller.phase == listRecordingAwaitingResponse {
		return codecs.ControlResponseFields{}, false
	}
	return poller.response, true
}

func (poller *RecordingDescriptorPoller) latchResponse(resp codecs.ControlResponseFields) {
	poller.response = resp
	if resp.Code != gensbe.ControlResponseCode_OK {
		poller.phase = listRecordingComplete
		return
	}
	poller.phase = listRecordingAwaitingDescriptor
	poller.tryComplete()
}

func (poller *RecordingDescriptorPoller) latchDescriptor(
	fields codecs.RecordingDescriptorFields,
) {
	poller.descriptor = codecs.CloneRecordingDescriptorFields(fields)
	poller.hasDescriptor = true
	poller.tryComplete()
}

func (poller *RecordingDescriptorPoller) tryComplete() {
	if poller.phase == listRecordingAwaitingDescriptor && poller.hasDescriptor {
		poller.phase = listRecordingComplete
	}
}

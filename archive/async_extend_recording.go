package archive

import "github.com/warrenguy/heron/archive/codecs/archivewire"

// AsyncExtendRecording is one non-blocking extend recording request.
//
// Behaviour: the handle completes on the correlated EXTEND
// RecordingSignalEvent for the requested recording id, not on the OK
// ControlResponse. AsyncStartRecording lists the call order, deadlines,
// caller rules, concurrency, lifetime, and ownership. They apply to this
// handle unchanged.
type AsyncExtendRecording struct {
	request        asyncSignalRequest
	tracker        ExtendSignalTracker
	streamID       int32
	sourceLocation archivewire.SourceLocation
	autoStop       archivewire.BooleanType
	channel        []byte
}

// NewAsyncExtendRecording builds one non-blocking extend recording request.
// NewAsyncExtendRecording draws one correlation id. ResetAwait on the session
// poller clears a prior latch, then arms the filter for that id. A private
// ExtendSignalTracker for that id and recordingID replaces the pending signal
// tracker on Client.
//
// autoStop: see ExtendRecordingAwaitSignal.
func (client *Client) NewAsyncExtendRecording(
	recordingID int64,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop bool,
	channel []byte,
) *AsyncExtendRecording {
	a := &AsyncExtendRecording{
		tracker: ExtendSignalTracker{
			controlSessionID: client.controlSessionID,
			recordingID:      recordingID,
		},
		streamID:       streamID,
		sourceLocation: sourceLocation,
		autoStop:       booleanTypeFromBool(autoStop),
		channel:        channel,
	}
	a.request = newAsyncSignalRequest(client, &a.tracker)
	return a
}

// TryOffer offers the ExtendRecordingRequest2 once with the stored
// correlation id. A nil or closed handle does not offer.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil or closed handle also returns false, nil
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncExtendRecording) TryOffer() (bool, error) {
	if a == nil || a.request.closed {
		return false, nil
	}
	client := a.request.client
	return controlOfferResult(client.proxy.TryExtendRecording(
		client.controlSessionID,
		a.request.correlationID,
		a.tracker.recordingID,
		a.streamID,
		a.sourceLocation,
		a.autoStop,
		a.channel,
	))
}

// PollSignal reports the EXTEND signal outcome for the stored correlation id.
// PollSignal reads the private tracker first, then the session poller
// capture. The tracker matches EXTEND only for the requested recording id.
//
// Returns:
//   - true, nil: EXTEND latched. Clears a matching latched terminal
//   - true, err: a matching error response arrived before EXTEND. err comes
//     from MapControlResponse. The capture stays latched until Close or the
//     next build
//   - false, nil: keep polling. Consumes a matching OK. Discards a latched
//     non-matching terminal as stale. A nil or closed handle also returns
//     false, nil
//
// The archive replies OK before it validates the joining image. A refusal
// arrives as a later error response. The demux drops that response while the
// OK stays latched. PollSignal returns the error only when the response
// arrives after PollSignal consumes the OK. Otherwise the caller deadline
// fails the request.
func (a *AsyncExtendRecording) PollSignal() (bool, error) {
	if a == nil {
		return false, nil
	}
	return a.request.pollSignal(a.tracker.Extended())
}

// RecordingID returns the requested recording id after the EXTEND signal.
//
// The id is valid only after PollSignal returns true, nil. Before that,
// RecordingID returns 0, which is also a valid catalog id.
func (a *AsyncExtendRecording) RecordingID() int64 {
	if a == nil || !a.request.proven {
		return 0
	}
	return a.tracker.ExtendedRecordingID()
}

// Close clears a matching latched terminal. Close disarms this handle's
// response filter. Close removes this handle's tracker from Client. A newer
// handle keeps its own filter and tracker.
//
// The archive may still extend the recording after Close. The caller owns the
// stop.
//
// Close is idempotent.
func (a *AsyncExtendRecording) Close() {
	if a == nil {
		return
	}
	a.request.close(&a.tracker)
}

package archive

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
)

// recordingSignalFanout sends each recording signal to the session handler,
// then to the pending signal tracker. The conversion from *Client allocates
// nothing.
type recordingSignalFanout Client

var _ RecordingSignalHandler = (*recordingSignalFanout)(nil)

// RecordingSignal calls a non-nil session handler, then a non-nil pending
// signal tracker.
//
// RecordingSignal reads the slot after the session handler returns. A Close
// inside that handler empties the slot first. A build inside that handler
// arms a fresh correlation id, which cannot match the current signal.
func (fanout *recordingSignalFanout) RecordingSignal(event codecs.RecordingSignalEventFields) {
	if handler := fanout.signalHandler; handler != nil {
		handler.RecordingSignal(event)
	}
	if tracker := fanout.pendingSignalTracker; tracker != nil {
		tracker.RecordingSignal(event)
	}
}

// signalTarget returns the recording signal target for Client.Poll and the
// ListRecording poll. An empty slot selects the session handler, so a nil
// handler still skips the signal decode. A full slot selects the fanout.
func (client *Client) signalTarget() RecordingSignalHandler {
	if client.pendingSignalTracker == nil {
		return client.signalHandler
	}
	return (*recordingSignalFanout)(client)
}

// correlatedSignalTracker is a recording signal tracker that matches one
// armed correlation id.
type correlatedSignalTracker interface {
	RecordingSignalHandler
	arm(correlationID int64)
}

// asyncSignalRequest holds the request state behind a non-blocking signal
// handle.
type asyncSignalRequest struct {
	client        *Client
	tracker       RecordingSignalHandler
	correlationID int64
	proven        bool
	closed        bool
}

// newAsyncSignalRequest arms the session poller filter and tracker for one
// fresh correlation id. tracker replaces any prior tracker in the slot.
func newAsyncSignalRequest(
	client *Client,
	tracker correlatedSignalTracker,
) asyncSignalRequest {
	correlationID := client.aeronClient.NextCorrelationID()
	client.controlPoller.ResetAwait(correlationID)
	tracker.arm(correlationID)
	client.pendingSignalTracker = tracker
	return asyncSignalRequest{
		client:        client,
		tracker:       tracker,
		correlationID: correlationID,
	}
}

// pollSignal returns the recordingSignalProof outcome for the stored
// correlation id. latched reports whether the tracker holds the signal. A
// closed request does not match.
//
// pollSignal marks the request proven for RecordingID when latched is true.
func (r *asyncSignalRequest) pollSignal(latched bool) (bool, error) {
	if r.closed {
		return false, nil
	}
	if latched {
		r.proven = true
	}
	return recordingSignalProof(
		r.client.controlPoller,
		latched,
		r.client.controlSessionID,
		r.correlationID,
	)
}

// close clears a matching latched terminal. close leaves the filter and slot
// of a newer request in place.
func (r *asyncSignalRequest) close() {
	if r.closed {
		return
	}
	r.closed = true
	client := r.client
	poller := client.controlPoller
	clearMatchingControlResponse(poller, client.controlSessionID, r.correlationID)
	poller.DisarmAwait(r.correlationID)
	if client.pendingSignalTracker == r.tracker {
		client.pendingSignalTracker = nil
	}
}

// controlOfferResult maps one Proxy Try result to the TryOffer contract.
func controlOfferResult(position int64, accepted bool) (bool, error) {
	if accepted {
		return true, nil
	}
	if aeron.IsIngressRetriable(position) {
		return false, nil
	}
	return false, &ControlTerminalError{Status: position}
}

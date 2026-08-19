package archive

import "github.com/warrenguy/heron/aeron"

// AsyncStopRecording is one non-blocking stop recording request.
//
// Behaviour: build the handle once. Call TryOffer and PollResponse across
// ticks. TryOffer offers StopRecordingRequest once per call. PollResponse
// reads the session poller capture. Client.Poll on the duty cycle feeds
// that capture. The handle never calls DoWork. The handle never polls the
// subscription.
//
// Concurrency: invoker-only with Client.Poll.
//
// Lifetime: the handle aliases channel until Close. The caller must not
// mutate the slice.
//
// Ownership: does not own the Client, publication, or poller.
type AsyncStopRecording struct {
	client        *Client
	streamID      int32
	channel       []byte
	correlationID int64
	closed        bool
}

// NewAsyncStopRecording builds one non-blocking stop recording request.
// The handle stores the assigned correlation id. ResetAwait on the session
// poller clears a prior latch, then arms the filter for that id.
func (client *Client) NewAsyncStopRecording(
	streamID int32,
	channel []byte,
) *AsyncStopRecording {
	correlationID := client.aeronClient.NextCorrelationID()
	client.controlPoller.ResetAwait(correlationID)
	return &AsyncStopRecording{
		client:        client,
		streamID:      streamID,
		channel:       channel,
		correlationID: correlationID,
	}
}

// TryOffer offers the StopRecordingRequest once with the stored correlation
// id. A nil or closed handle does not offer.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil or closed handle also returns false, nil
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncStopRecording) TryOffer() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	pos, ok := a.client.proxy.TryStopRecording(
		a.client.controlSessionID,
		a.correlationID,
		a.streamID,
		a.channel,
	)
	if ok {
		return true, nil
	}
	if aeron.IsIngressRetriable(pos) {
		return false, nil
	}
	return false, &ControlTerminalError{Status: pos}
}

// PollResponse reports the ControlResponse outcome for the stored
// correlation id from the session poller capture. Consumes a matching OK so
// the demux can accept later responses. Discards a latched non-matching
// terminal as stale. A matching error stays latched. Close or the next
// ResetAwait clears that latch.
//
// The duty cycle Client.Poll feeds the capture. Does not poll the
// subscription. A nil or closed handle does not match.
//
// Returns:
//   - true, nil: matching OK consumed
//   - true, err: matching error response, mapped through
//     MapControlResponse. Capture stays latched
//   - false, nil: no matching response captured yet, or the handle is
//     nil or closed
func (a *AsyncStopRecording) PollResponse() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	return consumeMatchedControlResponse(
		a.client.controlPoller,
		a.client.controlSessionID,
		a.correlationID,
	)
}

// Close clears a latched matching terminal. Disarms the response filter
// only when the poller is still armed for this handle's correlation id. A
// later ControlResponse may latch on a disarmed poller.
//
// Close is idempotent.
func (a *AsyncStopRecording) Close() {
	if a == nil || a.closed {
		return
	}
	a.closed = true
	poller := a.client.controlPoller
	clearMatchingControlResponse(
		poller,
		a.client.controlSessionID,
		a.correlationID,
	)
	poller.DisarmAwait(a.correlationID)
}

package archive

import "github.com/warrenguy/heron/archive/codecs/archivewire"

// AsyncStartRecording is one non-blocking start recording request.
//
// Behaviour: build the handle once. Call TryOffer until the publication
// accepts the request, then call PollSignal across ticks. Client.Poll on the
// duty cycle feeds the session poller capture and the handle's signal
// tracker. The handle completes on the correlated START RecordingSignalEvent,
// not on the OK ControlResponse. The handle never calls DoWork. The handle
// never polls the subscription.
//
// The handle keeps no clock. The caller bounds TryOffer retries with
// ControlOfferRetryTimeout. After an accepted offer, the caller bounds
// PollSignal with ControlResponseTimeout.
//
// Caller rules:
//   - Keep one outstanding request per Client, AsyncStopRecording included.
//     Close the older handle before the next build. Each build re-arms the
//     response filter. A start or extend build also takes the pending signal
//     tracker slot.
//   - Do not run a blocking call while a handle is open. A blocking call can
//     drop the handle's response or signal.
//   - Close the handle once PollSignal returns true. Until Close, the handle
//     claims ERROR responses for its correlation id.
//   - After Client.Close, TryOffer returns a terminal error. PollSignal and
//     Close stay safe.
//
// Concurrency: invoker-only with Client.Poll.
//
// Lifetime: the handle aliases channel until Close. The caller must not
// mutate the slice.
//
// Ownership: the caller must close every handle, including after an error or
// a deadline. An unclosed handle holds the pending signal tracker on Client
// until the next NewAsyncStartRecording or NewAsyncExtendRecording. The
// handle does not own the Client, publication, or poller.
type AsyncStartRecording struct {
	request        asyncSignalRequest
	tracker        StartSignalTracker
	streamID       int32
	sourceLocation archivewire.SourceLocation
	autoStop       archivewire.BooleanType
	channel        []byte
}

// NewAsyncStartRecording builds one non-blocking start recording request.
// NewAsyncStartRecording draws one correlation id. ResetAwait on the session
// poller clears a prior latch, then arms the filter for that id. A private
// StartSignalTracker for that id replaces the pending signal tracker on
// Client.
//
// autoStop: see StartRecording.
func (client *Client) NewAsyncStartRecording(
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop bool,
	channel []byte,
) *AsyncStartRecording {
	a := &AsyncStartRecording{
		tracker:        StartSignalTracker{controlSessionID: client.controlSessionID},
		streamID:       streamID,
		sourceLocation: sourceLocation,
		autoStop:       booleanTypeFromBool(autoStop),
		channel:        channel,
	}
	a.request = newAsyncSignalRequest(client, &a.tracker)
	return a
}

// TryOffer offers the StartRecordingRequest2 once with the stored
// correlation id. A nil or closed handle does not offer.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil or closed handle also returns false, nil
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncStartRecording) TryOffer() (bool, error) {
	if a == nil || a.request.closed {
		return false, nil
	}
	client := a.request.client
	return controlOfferResult(client.proxy.TryStartRecording(
		client.controlSessionID,
		a.request.correlationID,
		a.streamID,
		a.sourceLocation,
		a.autoStop,
		a.channel,
	))
}

// PollSignal reports the START signal outcome for the stored correlation id.
// PollSignal reads the private tracker first, then the session poller
// capture.
//
// Returns:
//   - true, nil: START latched. Clears a matching latched terminal
//   - true, err: a matching error response arrived before START. err comes
//     from MapControlResponse. The capture stays latched until Close or the
//     next build
//   - false, nil: keep polling. Consumes a matching OK. Discards a latched
//     non-matching terminal as stale. A nil or closed handle also returns
//     false, nil
//
// A latched OK holds a later matching error response in the image. After
// PollSignal consumes the OK, the next Client.Poll latches the error.
// PollSignal then returns the error unless START has latched.
func (a *AsyncStartRecording) PollSignal() (bool, error) {
	if a == nil {
		return false, nil
	}
	return a.request.pollSignal(a.tracker.Started())
}

// RecordingID returns the catalog recording id from the START signal.
//
// The id is valid only after PollSignal returns true, nil. Before that,
// RecordingID returns 0, which is also a valid catalog id.
func (a *AsyncStartRecording) RecordingID() int64 {
	if a == nil || !a.request.proven {
		return 0
	}
	return a.tracker.StartedRecordingID()
}

// Close clears a matching latched terminal. Close disarms this handle's
// response filter. Close removes this handle's tracker from Client. A newer
// handle keeps its own filter and tracker.
//
// The archive may still start the recording after Close. The caller owns the
// stop.
//
// Until Close, a latched terminal holds a later matching response in the
// image, with every fragment behind it. Call Close once PollSignal returns
// true.
//
// Close is idempotent.
func (a *AsyncStartRecording) Close() {
	if a == nil {
		return
	}
	a.request.close(&a.tracker)
}

package archive

// AsyncStopRecordingByIdentity is one non-blocking stop recording by identity
// request.
//
// Behaviour: build the handle once. Call TryOffer and PollResponse across
// ticks. Client.Poll on the duty cycle feeds the session poller capture that
// PollResponse reads. The handle never calls DoWork or polls the
// subscription.
//
// Caller rules:
//   - Keep one outstanding request per Client. Close the older handle before
//     the next build.
//   - Do not run a blocking call while the handle is open. A blocking call
//     re-arms the session poller and can drop the handle's response.
//
// Concurrency: invoker-only with Client.Poll.
//
// Ownership: does not own the Client, publication, or poller.
type AsyncStopRecordingByIdentity struct {
	request     asyncRequest
	recordingID int64
	stopped     bool
}

// NewAsyncStopRecordingByIdentity builds one handle for recordingID. The build
// clears a prior latch on the session poller.
func (client *Client) NewAsyncStopRecordingByIdentity(
	recordingID int64,
) *AsyncStopRecordingByIdentity {
	return &AsyncStopRecordingByIdentity{
		request:     newAsyncRequest(client),
		recordingID: recordingID,
	}
}

// TryOffer offers the StopRecordingByIdentityRequest once.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil, closed, or responded handle also returns false, nil
//     without an offer
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncStopRecordingByIdentity) TryOffer() (bool, error) {
	if a == nil || !a.request.canOffer() {
		return false, nil
	}
	client := a.request.client
	return controlOfferResult(client.proxy.TryStopRecordingByIdentity(
		client.controlSessionID,
		a.request.correlationID,
		a.recordingID,
	))
}

// PollResponse reports the ControlResponse outcome for the stored
// correlation id from the session poller capture. PollResponse consumes a
// matching OK so the demux can accept later responses. A non-zero relevantId
// in that OK sets the IsStopped result. After it consumes the OK, PollResponse
// leaves a later handle's latch in place. PollResponse discards a latched
// non-matching terminal as stale. A matching error stays latched until Close
// or the next ResetAwait.
//
// Returns:
//   - true, nil: matching OK consumed, now or on an earlier call
//   - true, err: matching error response, mapped through
//     MapControlResponse. An unknown recordingID yields an Exception with
//     ErrorCode ErrorUnknownRecording
//   - false, nil: no matching response yet, or the handle is nil or closed
func (a *AsyncStopRecordingByIdentity) PollResponse() (bool, error) {
	if a == nil {
		return false, nil
	}
	if done, settled := a.request.settled(); settled {
		return done, nil
	}
	request := &a.request
	fields, mapped, ok := tryMatchControlResponseDiscardStale(
		request.client.controlPoller,
		request.client.controlSessionID,
		request.correlationID,
	)
	if !ok {
		return false, nil
	}
	if mapped != nil {
		return true, mapped
	}
	a.stopped = fields.RelevantID != 0
	request.latchResponded()
	return true, nil
}

// IsStopped reports whether this request removed the recording subscription
// of an active recording. The result is valid once PollResponse returns true,
// nil, and stays true across later polls and Close. Before that, and on a
// nil handle, IsStopped returns false.
func (a *AsyncStopRecordingByIdentity) IsStopped() bool {
	return a != nil && a.stopped
}

// Close clears a latched matching terminal. Close disarms this handle's response
// filter, not a later handle's filter. A later ControlResponse may latch on a
// disarmed poller.
//
// Close is idempotent.
func (a *AsyncStopRecordingByIdentity) Close() {
	if a == nil {
		return
	}
	a.request.close()
}

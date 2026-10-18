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
	client        *Client
	recordingID   int64
	correlationID int64
	stopped       bool
	closed        bool
}

// NewAsyncStopRecordingByIdentity builds one handle for recordingID.
// ResetAwait on the session poller clears a prior latch, then arms the filter
// for the new correlation id.
func (client *Client) NewAsyncStopRecordingByIdentity(
	recordingID int64,
) *AsyncStopRecordingByIdentity {
	correlationID := client.aeronClient.NextCorrelationID()
	client.controlPoller.ResetAwait(correlationID)
	return &AsyncStopRecordingByIdentity{
		client:        client,
		recordingID:   recordingID,
		correlationID: correlationID,
	}
}

// TryOffer offers the StopRecordingByIdentityRequest once with the stored
// correlation id.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil or closed handle also returns false, nil
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncStopRecordingByIdentity) TryOffer() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	return controlOfferResult(a.client.proxy.TryStopRecordingByIdentity(
		a.client.controlSessionID,
		a.correlationID,
		a.recordingID,
	))
}

// PollResponse reports the ControlResponse outcome for the stored
// correlation id from the session poller capture. A matching OK with a
// non-zero relevantId latches the IsStopped result, then PollResponse
// consumes the OK so the demux can accept later responses. A later OK never
// clears a latched result. PollResponse discards a latched non-matching terminal as
// stale. A matching error stays latched until Close or the next ResetAwait.
//
// Returns:
//   - true, nil: matching OK consumed. IsStopped reports its result
//   - true, err: matching error response, mapped through
//     MapControlResponse. An unknown recordingID yields an Exception with
//     ErrorCode ErrorUnknownRecording. Capture stays latched
//   - false, nil: no matching response yet, or the handle is nil or closed
func (a *AsyncStopRecordingByIdentity) PollResponse() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	poller := a.client.controlPoller
	fields, mapped, ok := tryMatchControlResponseDiscardStale(
		poller,
		a.client.controlSessionID,
		a.correlationID,
	)
	if !ok {
		return false, nil
	}
	if mapped != nil {
		return true, mapped
	}
	a.stopped = a.stopped || fields.RelevantID != 0
	poller.ResetCapture()
	return true, nil
}

// IsStopped reports whether this request removed the recording subscription
// of an active recording. The result is valid once PollResponse returns true,
// nil, and stays true across later polls and Close. Before that, and on a
// nil handle, IsStopped returns false.
func (a *AsyncStopRecordingByIdentity) IsStopped() bool {
	return a != nil && a.stopped
}

// Close clears a latched matching terminal. Disarms the response filter
// only when the poller is still armed for this handle's correlation id. A
// later ControlResponse may latch on a disarmed poller.
//
// Close is idempotent.
func (a *AsyncStopRecordingByIdentity) Close() {
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

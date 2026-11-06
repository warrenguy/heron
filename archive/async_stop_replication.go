package archive

// AsyncStopReplication is one non-blocking stop replication request.
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
type AsyncStopReplication struct {
	request       asyncRequest
	replicationID int64
	stopped       bool
}

// NewAsyncStopReplication builds one handle for replicationID. The build clears
// a prior latch on the session poller.
func (client *Client) NewAsyncStopReplication(replicationID int64) *AsyncStopReplication {
	return &AsyncStopReplication{
		request:       newAsyncRequest(client),
		replicationID: replicationID,
	}
}

// TryOffer offers the StopReplicationRequest once.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil, closed, or responded handle also returns false, nil
//     without an offer
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncStopReplication) TryOffer() (bool, error) {
	if a == nil || !a.request.canOffer() {
		return false, nil
	}
	client := a.request.client
	return controlOfferResult(client.proxy.TryStopReplication(
		client.controlSessionID,
		a.request.correlationID,
		a.replicationID,
	))
}

// PollResponse reports the ControlResponse outcome for the stored
// correlation id from the session poller capture. A matching OK sets
// IsStopped. The stop OK carries RelevantID 0, so the code alone counts.
// PollResponse discards a latched non-matching terminal as stale. After it
// consumes a response, PollResponse leaves a later handle's latch in place.
//
// Returns:
//   - true, nil: matching OK or ErrorUnknownReplication consumed, now or on
//     an earlier call
//   - true, err: any other matching error response, mapped through
//     MapControlResponse. Capture stays latched until Close or the next
//     ResetAwait
//   - false, nil: no matching response yet, or the handle is nil or closed
func (a *AsyncStopReplication) PollResponse() (bool, error) {
	if a == nil {
		return false, nil
	}
	if done, settled := a.request.settled(); settled {
		return done, nil
	}
	matched, err := a.request.consumeResponse()
	if !matched {
		return false, nil
	}
	if err == nil {
		a.stopped = true
		return true, nil
	}
	if isUnknownReplication(err) {
		a.request.latchResponded()
		return true, nil
	}
	return true, err
}

// IsStopped reports whether this request stopped an active replication. The
// result is valid once PollResponse returns true, nil. It stays true across
// later polls and Close. Returns false before an OK, after
// ErrorUnknownReplication alone, and on a nil handle.
func (a *AsyncStopReplication) IsStopped() bool {
	return a != nil && a.stopped
}

// Close clears a latched matching terminal. Close disarms this handle's response
// filter, not a later handle's filter. A later ControlResponse may latch on a
// disarmed poller.
//
// Close is idempotent.
func (a *AsyncStopReplication) Close() {
	if a == nil {
		return
	}
	a.request.close()
}

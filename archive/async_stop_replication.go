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
	client        *Client
	replicationID int64
	correlationID int64
	stopped       bool
	closed        bool
}

// NewAsyncStopReplication builds one handle for replicationID.
// ResetAwait on the session poller clears a prior latch, then arms the filter
// for the new correlation id.
func (client *Client) NewAsyncStopReplication(replicationID int64) *AsyncStopReplication {
	correlationID := client.aeronClient.NextCorrelationID()
	client.controlPoller.ResetAwait(correlationID)
	return &AsyncStopReplication{
		client:        client,
		replicationID: replicationID,
		correlationID: correlationID,
	}
}

// TryOffer offers the StopReplicationRequest once with the stored
// correlation id.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil or closed handle also returns false, nil
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncStopReplication) TryOffer() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	return controlOfferResult(a.client.proxy.TryStopReplication(
		a.client.controlSessionID,
		a.correlationID,
		a.replicationID,
	))
}

// PollResponse reports the ControlResponse outcome for the stored
// correlation id from the session poller capture. A matching OK sets
// IsStopped. The stop OK carries RelevantID 0, so the code alone counts. A
// matching ErrorUnknownReplication is an outcome, not an error. PollResponse
// consumes both. PollResponse discards a latched non-matching terminal as
// stale. Any other matching error stays latched until Close or the next
// ResetAwait.
//
// Returns:
//   - true, nil: matching OK or ErrorUnknownReplication consumed. IsStopped
//     reports the result
//   - true, err: any other matching error response, mapped through
//     MapControlResponse. Capture stays latched
//   - false, nil: no matching response yet, or the handle is nil or closed
func (a *AsyncStopReplication) PollResponse() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	poller := a.client.controlPoller
	matched, err := consumeMatchedControlResponse(
		poller,
		a.client.controlSessionID,
		a.correlationID,
	)
	if !matched {
		return false, nil
	}
	if err == nil {
		a.stopped = true
		return true, nil
	}
	if isUnknownReplication(err) {
		poller.ResetCapture()
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

// Close clears a latched matching terminal. Disarms the response filter
// only when the poller is still armed for this handle's correlation id. A
// later ControlResponse may latch on a disarmed poller.
//
// Close is idempotent.
func (a *AsyncStopReplication) Close() {
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

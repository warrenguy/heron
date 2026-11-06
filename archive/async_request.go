package archive

// asyncRequest holds the request state behind a non-blocking handle that
// completes on its ControlResponse.
//
// responded stays true once the handle consumes its response. close still
// clears a matching latch.
type asyncRequest struct {
	client        *Client
	correlationID int64
	responded     bool
	closed        bool
}

// newAsyncRequest clears a prior latch on the session poller, then arms the
// filter for a new correlation id.
func newAsyncRequest(client *Client) asyncRequest {
	correlationID := client.aeronClient.NextCorrelationID()
	client.controlPoller.ResetAwait(correlationID)
	return asyncRequest{
		client:        client,
		correlationID: correlationID,
	}
}

func (r *asyncRequest) canOffer() bool {
	return !r.closed && !r.responded
}

// settled reports the PollResponse result of a request that no longer reads
// the capture. ok is false while the request awaits its response.
func (r *asyncRequest) settled() (done, ok bool) {
	if r.closed {
		return false, true
	}
	return r.responded, r.responded
}

// consumeResponse matches the latched ControlResponse for the stored
// correlation id. It discards a latched non-matching terminal. A consumed OK
// sets responded.
func (r *asyncRequest) consumeResponse() (bool, error) {
	matched, err := consumeMatchedControlResponse(
		r.client.controlPoller,
		r.client.controlSessionID,
		r.correlationID,
	)
	if matched && err == nil {
		r.responded = true
	}
	return matched, err
}

func (r *asyncRequest) latchResponded() {
	r.client.controlPoller.ResetCapture()
	r.responded = true
}

func (r *asyncRequest) close() {
	if r.closed {
		return
	}
	r.closed = true
	poller := r.client.controlPoller
	clearMatchingControlResponse(poller, r.client.controlSessionID, r.correlationID)
	poller.DisarmAwait(r.correlationID)
}

package archive

// AsyncStopRecording is one non-blocking stop recording request.
//
// Behaviour: build the handle once. Call TryOffer and PollResponse across
// ticks. Client.Poll on the duty cycle feeds the session poller capture that
// PollResponse reads. The handle never calls DoWork. The handle never polls
// the subscription.
//
// Caller rules:
//   - Keep one outstanding request per Client. Close the older handle before
//     the next build.
//   - Do not run a blocking call while the handle is open. A blocking call
//     re-arms the session poller and can drop the handle's response.
//
// Concurrency: invoker-only with Client.Poll.
//
// Lifetime: the handle aliases channel until Close. The caller must not
// mutate the slice.
//
// Ownership: does not own the Client, publication, or poller.
type AsyncStopRecording struct {
	request  asyncRequest
	streamID int32
	channel  []byte
}

// NewAsyncStopRecording builds one non-blocking stop recording request. The
// build clears a prior latch on the session poller.
func (client *Client) NewAsyncStopRecording(
	streamID int32,
	channel []byte,
) *AsyncStopRecording {
	return &AsyncStopRecording{
		request:  newAsyncRequest(client),
		streamID: streamID,
		channel:  channel,
	}
}

// TryOffer offers the StopRecordingRequest at most once per call.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil, closed, or responded handle also returns false, nil
//     without an offer
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncStopRecording) TryOffer() (bool, error) {
	if a == nil || !a.request.canOffer() {
		return false, nil
	}
	client := a.request.client
	return controlOfferResult(client.proxy.TryStopRecording(
		client.controlSessionID,
		a.request.correlationID,
		a.streamID,
		a.channel,
	))
}

// PollResponse reports the ControlResponse outcome for the stored
// correlation id from the session poller capture. Consumes a matching OK so
// the demux can accept later responses. After it consumes the OK, PollResponse
// leaves a later handle's latch in place. Discards a latched non-matching
// terminal as stale. A matching error stays latched. Close or the next
// ResetAwait clears that latch.
//
// Returns:
//   - true, nil: matching OK consumed, now or on an earlier call
//   - true, err: matching error response, mapped through MapControlResponse
//   - false, nil: no matching response captured yet, or the handle is
//     nil or closed
func (a *AsyncStopRecording) PollResponse() (bool, error) {
	if a == nil {
		return false, nil
	}
	if done, settled := a.request.settled(); settled {
		return done, nil
	}
	return a.request.consumeResponse()
}

// Close clears a latched matching terminal. Close disarms this handle's response
// filter, not a later handle's filter. A later ControlResponse may latch on a
// disarmed poller.
//
// Close is idempotent.
func (a *AsyncStopRecording) Close() {
	if a == nil {
		return
	}
	a.request.close()
}

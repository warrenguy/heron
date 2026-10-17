package archive

import "github.com/warrenguy/heron/archive/codecs"

// AsyncListRecording is one non-blocking list recording request.
//
// Behaviour: build the handle once. Call TryOffer and PollDescriptor across
// ticks. TryOffer offers ListRecordingRequest once per call. PollDescriptor
// reads the session descriptor poller and the session control poller.
// Client.Poll on the duty cycle feeds both captures. The handle never calls
// DoWork. The handle never polls the subscription.
//
// Caller rules:
//   - Keep one outstanding request per Client. Close the older handle before
//     the next build.
//   - Do not run a blocking call while the handle is open. A blocking call
//     re-arms the pollers and can drop the handle's descriptor or response.
//
// Concurrency: invoker-only with Client.Poll.
//
// Ownership: does not own the Client, publication, or pollers.
type AsyncListRecording struct {
	client        *Client
	recordingID   int64
	correlationID int64
	closed        bool
}

// NewAsyncListRecording builds one non-blocking list recording request for
// recordingID. NewAsyncListRecording draws one correlation id. ResetAwait on
// both session pollers clears a prior latch, then arms each filter for that
// id.
func (client *Client) NewAsyncListRecording(recordingID int64) *AsyncListRecording {
	correlationID := client.aeronClient.NextCorrelationID()
	client.controlPoller.ResetAwait(correlationID)
	client.descriptorPoller.ResetAwait(correlationID)
	return &AsyncListRecording{
		client:        client,
		recordingID:   recordingID,
		correlationID: correlationID,
	}
}

// TryOffer offers the ListRecordingRequest once with the stored correlation
// id. A nil or closed handle does not offer.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil or closed handle also returns false, nil
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncListRecording) TryOffer() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	return controlOfferResult(a.client.proxy.TryListRecording(
		a.client.controlSessionID,
		a.correlationID,
		a.recordingID,
	))
}

// PollDescriptor reports the outcome for the stored correlation id. A
// matching RecordingDescriptor wins over a matching ControlResponse. Consumes
// a matching OK and keeps waiting for the descriptor. Discards a latched
// non-matching terminal as stale. A matching error stays latched until the
// descriptor completes the handle or Close clears it.
//
// The duty cycle Client.Poll feeds both captures. Does not poll the
// subscription. A nil or closed handle does not match.
//
// Returns:
//   - true, descriptor, nil: matching descriptor captured. Its var-ascii
//     fields stay valid after later polls
//   - true, zero, err: matching non-OK response, mapped through
//     MapControlResponse. RECORDING_UNKNOWN maps to ErrorUnknownRecording
//   - false, zero, nil: no outcome yet, or the handle is nil or closed
func (a *AsyncListRecording) PollDescriptor() (
	done bool,
	descriptor codecs.RecordingDescriptorFields,
	err error,
) {
	if a == nil || a.closed {
		return false, codecs.RecordingDescriptorFields{}, nil
	}
	controlPoller := a.client.controlPoller
	descriptorPoller := a.client.descriptorPoller
	descriptorLatched := descriptorPoller.awaitCorrelationID == a.correlationID &&
		descriptorPoller.IsPollComplete()
	if descriptorLatched {
		clearMatchingControlResponse(
			controlPoller,
			a.client.controlSessionID,
			a.correlationID,
		)
		descriptor, err = descriptorPollerOutcome(descriptorPoller)
		return true, descriptor, err
	}
	done, err = consumeAwaitingControlResponse(
		controlPoller,
		a.client.controlSessionID,
		a.correlationID,
	)
	return done, codecs.RecordingDescriptorFields{}, err
}

// Close clears a latched matching control terminal. Close disarms each
// poller filter that still holds this handle's correlation id. Disarming the
// descriptor poller also drops its capture.
//
// Close is idempotent.
func (a *AsyncListRecording) Close() {
	if a == nil || a.closed {
		return
	}
	a.closed = true
	controlPoller := a.client.controlPoller
	clearMatchingControlResponse(
		controlPoller,
		a.client.controlSessionID,
		a.correlationID,
	)
	controlPoller.DisarmAwait(a.correlationID)
	a.client.descriptorPoller.disarm(a.correlationID)
}

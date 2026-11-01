package archive

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

// AsyncReplicate is one non-blocking replicate request plus the recording
// signals of its replication.
//
// Behaviour: build the handle once. Call TryOffer and PollResponse across
// ticks. Client.Poll on the duty cycle feeds the session poller capture that
// PollResponse reads. The OK completes the request. Its RelevantID is the
// replication id. Recording signals keyed on that id then report progress.
// The handle never calls DoWork or polls the subscription.
//
// Signals: the handle implements RecordingSignalHandler. Route session
// signals to it, for example through SetRecordingSignalHandler. The handle
// folds a signal only when its correlation id equals the replication id. One
// Client.Poll can carry the OK and the first signals. The handle reads the
// replication id from the latched OK before PollResponse consumes it.
//
// Caller rules:
//   - Keep one outstanding request per Client until PollResponse returns
//     true, nil. The handle then stops reading responses. Other requests may
//     run.
//   - After PollResponse returns an error, Close the handle before the next
//     build. An open handle can discard the response of a later request.
//   - After a true TryOffer, call PollResponse after each Client.Poll until
//     it returns true. Until then the latched OK holds back later responses.
//   - Keep the handle open from a true TryOffer to a true PollResponse. A
//     handle closed before the OK latches keeps ReplicationID at -1. The
//     caller then cannot stop the replication. The archive runs it until
//     StopPosition, a source failure, or its own close.
//   - Close reads an OK that has already latched. The caller may stop by
//     ReplicationID after Close.
//   - Do not run an *AwaitSignal call while the handle is open. Those calls
//     route signals to their own tracker. The handle misses them.
//   - An AsyncStartRecording or AsyncExtendRecording may share the Client
//     once PollResponse returned true. Their signal tracker does not affect
//     this handle.
//
// Replication failure: the archive reports a failed replication as an ERROR
// keyed on the replication id. A non-nil error response handler receives the
// ERROR. The client does not deliver the ERROR when the handler is nil. The
// handle sees REPLICATE_END, and STOP when a destination recording session
// started. Bound the replication with a progress deadline.
//
// Concurrency: invoker-only with Client.Poll.
//
// Lifetime: the handle aliases sourceControlChannel and the slices in params
// until Close.
//
// Ownership: does not own the Client, the publication, the poller, or the
// replication. Close does not stop the replication.
type AsyncReplicate struct {
	client                *Client
	sourceRecordingID     int64
	sourceControlStreamID int32
	sourceControlChannel  []byte
	params                ReplicationParams
	correlationID         int64
	replicationID         int64
	recordingID           int64
	position              int64
	responded             bool
	extended              bool
	synced                bool
	merged                bool
	stopped               bool
	replicationEnded      bool
	recordingDeleted      bool
	closed                bool
}

var _ RecordingSignalHandler = (*AsyncReplicate)(nil)

// NewAsyncReplicate builds one handle that asks the archive to replicate
// sourceRecordingID from the archive at sourceControlChannel and
// sourceControlStreamID. Build params with NewReplicationParams.
//
// ResetAwait on the session poller clears a prior latch, then arms the filter
// for the new correlation id. NewAsyncReplicate does not touch the pending
// signal tracker of an open AsyncStartRecording or AsyncExtendRecording.
func (client *Client) NewAsyncReplicate(
	sourceRecordingID int64,
	sourceControlStreamID int32,
	sourceControlChannel []byte,
	params ReplicationParams,
) *AsyncReplicate {
	correlationID := client.aeronClient.NextCorrelationID()
	client.controlPoller.ResetAwait(correlationID)
	return &AsyncReplicate{
		client:                client,
		sourceRecordingID:     sourceRecordingID,
		sourceControlStreamID: sourceControlStreamID,
		sourceControlChannel:  sourceControlChannel,
		params:                params,
		correlationID:         correlationID,
		replicationID:         cnc.NullValue,
		recordingID:           cnc.NullValue,
		position:              NullPosition,
	}
}

// TryOffer validates params, then offers the ReplicateRequest2 once with the
// stored correlation id.
//
// Returns:
//   - true, nil: the publication accepted the offer
//   - false, nil: retriable (IsIngressRetriable). Offer again on a later
//     call. A nil or closed handle also returns false, nil
//   - false, err: params fail validation. err wraps
//     ErrInvalidReplicationParams. The handle does not offer
//   - false, err: any !IsIngressRetriable result as ControlTerminalError,
//     including NotConnected and encode failure (Status == 0)
func (a *AsyncReplicate) TryOffer() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	if err := a.params.validate(); err != nil {
		return false, err
	}
	return controlOfferResult(a.client.proxy.TryReplicate(
		a.client.controlSessionID,
		a.correlationID,
		a.sourceRecordingID,
		a.sourceControlStreamID,
		a.sourceControlChannel,
		a.params,
	))
}

// PollResponse reports the ControlResponse outcome for the stored
// correlation id from the session poller capture. PollResponse discards a
// latched non-matching terminal as stale.
//
// A matching OK stores its RelevantID as ReplicationID. PollResponse then
// consumes the OK. It disarms the filter for its own correlation id only.
// Every later call returns true, nil without reading the capture.
//
// Returns:
//   - true, nil: matching OK consumed. ReplicationID holds the replication id
//   - true, err: matching error response, mapped through MapControlResponse.
//     The capture stays latched until Close or the next ResetAwait
//   - false, nil: no matching response yet, or the handle is nil or closed
func (a *AsyncReplicate) PollResponse() (bool, error) {
	if a == nil || a.closed {
		return false, nil
	}
	if a.responded {
		return true, nil
	}
	a.resolveReplicationID()
	poller := a.client.controlPoller
	matched, err := consumeMatchedControlResponse(
		poller,
		a.client.controlSessionID,
		a.correlationID,
	)
	if !matched || err != nil {
		return matched, err
	}
	a.responded = true
	poller.DisarmAwait(a.correlationID)
	return true, nil
}

// RecordingSignal folds one signal of this replication.
//
// Behaviour:
//   - a nil or closed handle drops the event
//   - the handle does not check ControlSessionID. Client.Poll drops a
//     foreign session first. A rebuilt event may carry 0
//   - while the replication id is unknown, RecordingSignal reads it from
//     this handle's latched OK. Until then it drops every event
//   - RecordingSignal drops an event whose CorrelationID differs from the
//     replication id
//
// A matching event sets the flag of its kind. A flag never turns false again.
// REPLICATE and START do not set a flag. A RecordingID other than -1 and a
// Position other than NullPosition replace the stored values. RecordingSignal
// does not allocate.
func (a *AsyncReplicate) RecordingSignal(event codecs.RecordingSignalEventFields) {
	if a == nil || a.closed {
		return
	}
	a.resolveReplicationID()
	replicationID := a.replicationID
	if replicationID == cnc.NullValue || event.CorrelationID != replicationID {
		return
	}
	switch event.Signal {
	case archivewire.RecordingSignal_EXTEND:
		a.extended = true
	case archivewire.RecordingSignal_SYNC:
		a.synced = true
	case archivewire.RecordingSignal_MERGE:
		a.merged = true
	case archivewire.RecordingSignal_STOP:
		a.stopped = true
	case archivewire.RecordingSignal_REPLICATE_END:
		a.replicationEnded = true
	case archivewire.RecordingSignal_DELETE:
		a.recordingDeleted = true
	case archivewire.RecordingSignal_REPLICATE,
		archivewire.RecordingSignal_START,
		archivewire.RecordingSignal_NULL_VALUE:
		// These kinds do not set a flag.
	}
	if event.RecordingID != cnc.NullValue {
		a.recordingID = event.RecordingID
	}
	if event.Position != NullPosition {
		a.position = event.Position
	}
}

// resolveReplicationID reads an unknown replication id from this handle's
// latched OK. The demux latches the OK before it dispatches the signals
// behind it in the same poll. A matching ERROR leaves the id unknown. A
// closed handle does not read. The check compares the code without
// MapControlResponse, so it does not allocate.
func (a *AsyncReplicate) resolveReplicationID() {
	if a.closed || a.replicationID != cnc.NullValue {
		return
	}
	client := a.client
	fields, ok := client.controlPoller.Response()
	if !ok {
		return
	}
	ownOK := fields.ControlSessionID == client.controlSessionID &&
		fields.CorrelationID == a.correlationID &&
		fields.Code == archivewire.ControlResponseCode_OK
	if ownOK {
		a.replicationID = fields.RelevantID
	}
}

// ReplicationID returns the replication id from the latched OK, before or
// after PollResponse consumes it. Returns -1 before the OK latches, after an
// ERROR, and on a nil handle. The value stays after Close.
func (a *AsyncReplicate) ReplicationID() int64 {
	if a == nil {
		return cnc.NullValue
	}
	a.resolveReplicationID()
	return a.replicationID
}

// RecordingID returns the destination recording id from the last matching
// signal. Returns -1 until a signal carries one, and on a nil handle.
func (a *AsyncReplicate) RecordingID() int64 {
	if a == nil {
		return cnc.NullValue
	}
	return a.recordingID
}

// Position returns the position from the last matching signal. Returns
// NullPosition until a signal carries one, and on a nil handle.
func (a *AsyncReplicate) Position() int64 {
	if a == nil {
		return NullPosition
	}
	return a.position
}

// HasExtended reports whether a matching EXTEND arrived. The destination
// recording then accepts data from the replication.
func (a *AsyncReplicate) HasExtended() bool {
	return a != nil && a.extended
}

// HasSynced reports whether a matching SYNC arrived. The destination
// recording then holds the source recording up to its stop position.
func (a *AsyncReplicate) HasSynced() bool {
	return a != nil && a.synced
}

// HasMerged reports whether a matching MERGE arrived. The replication then
// follows LiveDestination.
func (a *AsyncReplicate) HasMerged() bool {
	return a != nil && a.merged
}

// HasStopped reports whether a matching STOP arrived. The destination
// recording session has ended.
func (a *AsyncReplicate) HasStopped() bool {
	return a != nil && a.stopped
}

// HasReplicationEnded reports whether a matching REPLICATE_END arrived. The
// archive has closed the replication.
func (a *AsyncReplicate) HasReplicationEnded() bool {
	return a != nil && a.replicationEnded
}

// IsRecordingDeleted reports whether a matching DELETE arrived.
func (a *AsyncReplicate) IsRecordingDeleted() bool {
	return a != nil && a.recordingDeleted
}

// Close first reads a latched matching OK into ReplicationID. Close then
// clears a latched matching terminal. Close disarms the response filter only
// when the poller is still armed for this handle's correlation id.
//
// Close does not stop the replication. The accessors keep their last
// values. Close is idempotent.
func (a *AsyncReplicate) Close() {
	if a == nil || a.closed {
		return
	}
	a.resolveReplicationID()
	a.closed = true
	poller := a.client.controlPoller
	clearMatchingControlResponse(poller, a.client.controlSessionID, a.correlationID)
	poller.DisarmAwait(a.correlationID)
}

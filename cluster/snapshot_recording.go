package cluster

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/consensus"
)

var (
	_ consensus.SnapshotRecordingControl = (*snapshotRecording)(nil)
	_ consensus.SnapshotPublicationAdder = snapshotPublicationAdder{}
	_ consensus.SnapshotStartRequest     = (*archive.AsyncStartRecording)(nil)
	_ consensus.SnapshotPublication      = (*aeron.ExclusivePublication)(nil)
)

// snapshotRecording is the module snapshot recording control over the
// module archive client.
//
// Behaviour: each start is one LOCAL async start with autoStop true. The STOP
// watch matches the module control session and the watched recording id.
// The module session handler passes each signal to handleRecordingSignal
// after the log recording handler.
//
// Concurrency: invoker-only. archive.Client.Poll calls handleRecordingSignal
// on the duty cycle.
//
// Ownership: does not own the archive client.
type snapshotRecording struct {
	offerRetryTimeoutNs int64
	responseTimeoutNs   int64

	// controlSessionID reads the module control session id. WatchStop calls
	// it. Tests substitute.
	controlSessionID func() int64

	// newStart builds one start request. Closed over the archive client at
	// newSnapshotRecording. Tests substitute.
	newStart func(streamID int32, channel string) consensus.SnapshotStartRequest

	// stop is the STOP watch, or nil when no watch is on.
	stop *archive.StopSignalTracker
}

// newSnapshotRecording returns the snapshot recording control over client.
// The control reads the control timeouts once. Each WatchStop reads the
// control session id from client.
func newSnapshotRecording(client *archive.Client) *snapshotRecording {
	cfg := client.Config()
	return &snapshotRecording{
		offerRetryTimeoutNs: cfg.ControlOfferRetryTimeout.Nanoseconds(),
		responseTimeoutNs:   cfg.ControlResponseTimeout.Nanoseconds(),
		controlSessionID:    client.ControlSessionID,
		newStart: func(streamID int32, channel string) consensus.SnapshotStartRequest {
			return client.NewAsyncStartRecording(
				streamID,
				archivewire.SourceLocation_LOCAL,
				true,
				[]byte(channel),
			)
		},
	}
}

// NewStartRecording builds one LOCAL start with autoStop true on channel.
func (r *snapshotRecording) NewStartRecording(
	streamID int32,
	channel string,
) consensus.SnapshotStartRequest {
	return r.newStart(streamID, channel)
}

// ControlTimeoutsNs returns the archive offer retry and response timeouts in
// nanoseconds.
func (r *snapshotRecording) ControlTimeoutsNs() (offerRetryTimeoutNs, responseTimeoutNs int64) {
	return r.offerRetryTimeoutNs, r.responseTimeoutNs
}

// WatchStop starts a STOP watch for recordingID on the module control
// session. It replaces an earlier watch.
func (r *snapshotRecording) WatchStop(recordingID int64) {
	r.stop = archive.NewStopSignalTracker(r.controlSessionID(), recordingID)
}

// StopPosition returns the STOP position of the watched recording.
//
// Returns 0, false with no watch on or no STOP seen.
func (r *snapshotRecording) StopPosition() (stopPosition int64, stopped bool) {
	stop := r.stop
	if stop == nil || !stop.Stopped() {
		return 0, false
	}
	return stop.StoppedPosition(), true
}

// ClearStop ends the watch. Later signals do not reach a watch. Idempotent.
func (r *snapshotRecording) ClearStop() {
	r.stop = nil
}

// handleRecordingSignal passes event to the STOP watch when one is on.
func (r *snapshotRecording) handleRecordingSignal(event codecs.RecordingSignalEventFields) {
	if r.stop != nil {
		r.stop.RecordingSignal(event)
	}
}

// snapshotPublicationAdder adds the module snapshot publication on the
// transport client.
//
// Concurrency: invoker-only.
//
// Ownership: does not own the client.
type snapshotPublicationAdder struct {
	client *aeron.Client
}

// OfferAddExclusivePublication forwards to the client.
func (a snapshotPublicationAdder) OfferAddExclusivePublication(
	streamID int32,
	channel string,
) (int64, error) {
	return a.client.OfferAddExclusivePublication(streamID, channel)
}

// PollAddExclusivePublication forwards to the client.
//
// Returns a nil publication unless done is true and err is nil. A nil
// *aeron.ExclusivePublication inside the interface would read as non-nil.
func (a snapshotPublicationAdder) PollAddExclusivePublication(
	correlationID int64,
) (consensus.SnapshotPublication, bool, error) {
	publication, done, err := a.client.PollAddExclusivePublication(correlationID)
	if !done || err != nil || publication == nil {
		return nil, done, err
	}
	return publication, true, nil
}

// CancelPending forwards to the client.
func (a snapshotPublicationAdder) CancelPending(correlationID int64) {
	a.client.CancelPending(correlationID)
}

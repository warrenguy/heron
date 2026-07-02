package client

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

// archiveControlOps bundles session deps for blocking archive control lifecycle helpers.
type archiveControlOps struct {
	aeronClient      *aeron.Client
	ctx              ArchiveContext
	proxy            *ArchiveProxy
	poll             func() int
	responsePoller   *ControlResponsePoller
	descriptorPoller *RecordingDescriptorPoller
	controlSessionID int64
}

func (archive *AeronArchive) sessionControlOps(pollFn func() int) archiveControlOps {
	return archiveControlOps{
		aeronClient:      archive.aeronClient,
		ctx:              archive.ctx,
		proxy:            archive.proxy,
		poll:             pollFn,
		responsePoller:   archive.controlPoller,
		descriptorPoller: archive.descriptorPoller,
		controlSessionID: archive.controlSessionID,
	}
}

// StartRecording offers a StartRecordingRequest and blocks until the archive replies
// with a matching ControlResponse, returning the assigned recording id.
func (ops archiveControlOps) startRecording(
	streamID int32,
	sourceLocation gensbe.SourceLocation,
	channel []byte,
) (int64, error) {
	correlationID := ops.aeronClient.NextCorrelationID()
	fields, err := offerAndAwaitArchiveControlRequest(
		ops.aeronClient,
		ops.ctx,
		ops.poll,
		ops.responsePoller,
		func() (int64, bool) {
			return ops.proxy.TryStartRecording(
				ops.controlSessionID,
				correlationID,
				streamID,
				sourceLocation,
				channel,
			)
		},
		ops.controlSessionID,
		correlationID,
	)
	if err != nil {
		return 0, err
	}
	return fields.RelevantID, nil
}

// StopRecording offers a StopRecordingRequest and blocks until the archive replies with
// a matching OK ControlResponse.
func (ops archiveControlOps) stopRecording(streamID int32, channel []byte) error {
	correlationID := ops.aeronClient.NextCorrelationID()
	_, err := offerAndAwaitArchiveControlRequest(
		ops.aeronClient,
		ops.ctx,
		ops.poll,
		ops.responsePoller,
		func() (int64, bool) {
			return ops.proxy.TryStopRecording(
				ops.controlSessionID,
				correlationID,
				streamID,
				channel,
			)
		},
		ops.controlSessionID,
		correlationID,
	)
	return err
}

// ListRecording offers a ListRecordingRequest and blocks until the archive replies with
// an OK ControlResponse and the matching RecordingDescriptor, returning its fields with
// var-ascii bytes cloned at latch time for retention.
func (ops archiveControlOps) listRecording(
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	if ops.descriptorPoller == nil {
		return codecs.RecordingDescriptorFields{}, errNilDescriptorPoller
	}
	correlationID := ops.aeronClient.NextCorrelationID()
	ops.descriptorPoller.ResetAwait(correlationID)
	return offerAndAwaitListRecording(
		ops.aeronClient,
		ops.ctx,
		ops.poll,
		ops.descriptorPoller,
		func() (int64, bool) {
			return ops.proxy.TryListRecording(
				ops.controlSessionID,
				correlationID,
				recordingID,
			)
		},
		correlationID,
	)
}

// StopRecordingAwaitSignal offers a StopRecordingRequest and blocks until the archive
// proves the stop with either a matching OK ControlResponse or a STOP
// RecordingSignalEvent recorded by tracker.
func (ops archiveControlOps) stopRecordingAwaitSignal(
	streamID int32,
	channel []byte,
	targets ArchiveControlPollTargets,
	tracker *StopSignalTracker,
) error {
	if tracker == nil {
		return errors.New("archive client: stop recording await signal: tracker must be non-nil")
	}
	tracker.Reset()
	if targets.ResponsePoller != nil {
		targets.ResponsePoller.ResetCapture()
	}
	correlationID := ops.aeronClient.NextCorrelationID()
	if _, err := offerArchiveControlRequest(ops.aeronClient, ops.ctx, ops.poll, func() (int64, bool) {
		return ops.proxy.TryStopRecording(
			ops.controlSessionID,
			correlationID,
			streamID,
			channel,
		)
	}); err != nil {
		return err
	}

	// ControlOfferUntilPositive drives poll during the offer retry, so a proof may
	// already be captured before the await loop begins.
	return runArchiveControlAwaitLoop(
		ops.aeronClient,
		ops.ctx,
		ops.poll,
		ops.ctx.MessageTimeout,
		func() (bool, error) {
			return stopRecordingProof(
				targets.ResponsePoller,
				tracker,
				ops.controlSessionID,
				correlationID,
			)
		},
		func() error {
			return fmt.Errorf(
				"archive client: stop recording timeout: correlation_id=%d",
				correlationID,
			)
		},
	)
}

// stopRecordingProof reports whether the stop completed and, on a ControlResponse
// proof, the mapped response error. A matching OK or error ControlResponse and a
// recorded STOP signal both complete; the signal proof carries a nil error.
func stopRecordingProof(
	poller *ControlResponsePoller,
	tracker *StopSignalTracker,
	controlSessionID int64,
	correlationID int64,
) (done bool, err error) {
	if tracker.Stopped() {
		return true, nil
	}
	if _, mapped, ok := TryMatchControlResponse(poller, controlSessionID, correlationID); ok {
		return true, mapped
	}
	return false, nil
}

// StopSignalTracker records the STOP RecordingSignalEvent for a target control session
// and recording. It implements RecordingSignalHandler for StopRecordingAwaitSignal.
type StopSignalTracker struct {
	controlSessionID   int64
	recordingID        int64
	stopped            bool
	stoppedRecordingID int64
	stoppedPosition    int64
}

var _ RecordingSignalHandler = (*StopSignalTracker)(nil)

// NewStopSignalTracker returns a tracker matching STOP signals for controlSessionID
// (0 matches all sessions) and recordingID (negative matches any recording).
func NewStopSignalTracker(controlSessionID, recordingID int64) *StopSignalTracker {
	return &StopSignalTracker{
		controlSessionID: controlSessionID,
		recordingID:      recordingID,
	}
}

// Reset clears prior STOP capture so the tracker can prove a new stop await.
func (t *StopSignalTracker) Reset() {
	t.stopped = false
	t.stoppedRecordingID = 0
	t.stoppedPosition = 0
}

// RecordingSignal records the first matching STOP signal.
func (t *StopSignalTracker) RecordingSignal(event codecs.RecordingSignalEventFields) {
	if event.Signal != gensbe.RecordingSignal_STOP {
		return
	}
	if t.controlSessionID != 0 && event.ControlSessionID != t.controlSessionID {
		return
	}
	if t.recordingID >= 0 && event.RecordingID != t.recordingID {
		return
	}
	if t.stopped {
		return
	}
	t.stopped = true
	t.stoppedRecordingID = event.RecordingID
	t.stoppedPosition = event.Position
}

// Stopped reports whether a matching STOP signal was recorded.
func (t *StopSignalTracker) Stopped() bool {
	return t.stopped
}

// StoppedRecordingID returns the recording id from the recorded STOP signal, or 0 when
// none was recorded.
func (t *StopSignalTracker) StoppedRecordingID() int64 {
	return t.stoppedRecordingID
}

// StoppedPosition returns the position from the recorded STOP signal, or 0 when none
// was recorded.
func (t *StopSignalTracker) StoppedPosition() int64 {
	return t.stoppedPosition
}

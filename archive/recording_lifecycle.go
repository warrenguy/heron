package archive

import (
	"context"
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

// archiveControlOps bundles session deps for blocking archive control lifecycle helpers.
type archiveControlOps struct {
	aeronClient      *aeron.Client
	ctx              Context
	proxy            *Proxy
	poll             func() int
	responsePoller   *ControlResponsePoller
	descriptorPoller *RecordingDescriptorPoller
	controlSessionID int64
}

func (client *Client) sessionControlOps(pollFn func() int) archiveControlOps {
	return archiveControlOps{
		aeronClient:      client.aeronClient,
		ctx:              client.ctx,
		proxy:            client.proxy,
		poll:             pollFn,
		responsePoller:   client.controlPoller,
		descriptorPoller: client.descriptorPoller,
		controlSessionID: client.controlSessionID,
	}
}

func (ops archiveControlOps) offerAndAwait(
	ctx context.Context,
	offer func(correlationID int64) (int64, bool),
) (codecs.ControlResponseFields, error) {
	correlationID := ops.aeronClient.NextCorrelationID()
	return offerAndAwaitControlRequest(
		ctx,
		ops.aeronClient,
		ops.ctx,
		ops.poll,
		ops.responsePoller,
		func() (int64, bool) { return offer(correlationID) },
		ops.controlSessionID,
		correlationID,
	)
}

// startRecording offers a StartRecordingRequest and blocks until the archive replies
// with a matching ControlResponse. RelevantID semantics: see Client.StartRecording.
func (ops archiveControlOps) startRecording(
	ctx context.Context,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	channel []byte,
) (int64, error) {
	fields, err := ops.offerAndAwait(ctx, func(correlationID int64) (int64, bool) {
		return ops.proxy.TryStartRecording(
			ops.controlSessionID,
			correlationID,
			streamID,
			sourceLocation,
			channel,
		)
	})
	if err != nil {
		return 0, err
	}
	return fields.RelevantID, nil
}

// StopRecording offers a StopRecordingRequest and blocks until the archive replies with
// a matching OK ControlResponse.
func (ops archiveControlOps) stopRecording(
	ctx context.Context,
	streamID int32,
	channel []byte,
) error {
	_, err := ops.offerAndAwait(ctx, func(correlationID int64) (int64, bool) {
		return ops.proxy.TryStopRecording(
			ops.controlSessionID,
			correlationID,
			streamID,
			channel,
		)
	})
	return err
}

// startReplay offers a ReplayRequest and blocks until the archive replies with a
// matching ControlResponse.
func (ops archiveControlOps) startReplay(
	ctx context.Context,
	recordingID int64,
	position int64,
	length int64,
	replayStreamID int32,
	replayChannel []byte,
) (int64, error) {
	fields, err := ops.offerAndAwait(ctx, func(correlationID int64) (int64, bool) {
		return ops.proxy.TryReplay(
			ops.controlSessionID,
			correlationID,
			recordingID,
			position,
			length,
			replayStreamID,
			replayChannel,
		)
	})
	if err != nil {
		return 0, err
	}
	return fields.RelevantID, nil
}

// stopReplay offers a StopReplayRequest and blocks until the archive replies with a
// matching OK ControlResponse.
func (ops archiveControlOps) stopReplay(ctx context.Context, replaySessionID int64) error {
	_, err := ops.offerAndAwait(ctx, func(correlationID int64) (int64, bool) {
		return ops.proxy.TryStopReplay(
			ops.controlSessionID,
			correlationID,
			replaySessionID,
		)
	})
	return err
}

// ListRecording offers a ListRecordingRequest and blocks until the archive replies with
// the matching RecordingDescriptor (or a non-OK ControlResponse), returning its fields
// with var-ascii bytes cloned at latch time for retention.
func (ops archiveControlOps) listRecording(
	ctx context.Context,
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	if ops.descriptorPoller == nil {
		return codecs.RecordingDescriptorFields{}, errNilDescriptorPoller
	}
	correlationID := ops.aeronClient.NextCorrelationID()
	ops.descriptorPoller.ResetAwait(correlationID)
	return offerAndAwaitListRecording(
		ctx,
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
	)
}

// StopRecordingAwaitSignal offers a StopRecordingRequest and blocks until the archive
// proves the stop with either a matching OK ControlResponse or a STOP
// RecordingSignalEvent recorded by tracker.
func (ops archiveControlOps) stopRecordingAwaitSignal(
	ctx context.Context,
	streamID int32,
	channel []byte,
	targets ControlPollTargets,
	tracker *StopSignalTracker,
) error {
	if tracker == nil {
		return errors.New("archive: stop recording await signal: tracker must be non-nil")
	}
	tracker.Reset()
	if targets.ResponsePoller != nil {
		targets.ResponsePoller.ResetCapture()
	}
	correlationID := ops.aeronClient.NextCorrelationID()
	if err := offerControlRequest(
		ctx, ops.aeronClient, ops.ctx, ops.poll, func() (int64, bool) {
			return ops.proxy.TryStopRecording(
				ops.controlSessionID,
				correlationID,
				streamID,
				channel,
			)
		},
	); err != nil {
		return err
	}

	// controlOfferUntilPositive drives poll during the offer retry, so a proof may
	// already be captured before the await loop begins.
	return runControlAwaitLoop(
		ctx,
		ops.aeronClient,
		ops.ctx,
		ops.poll,
		ops.ctx.ControlResponseTimeout,
		correlationID,
		StopRecordingAwaitOperation,
		func() (bool, error) {
			return stopRecordingProof(
				targets.ResponsePoller,
				tracker,
				ops.controlSessionID,
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
	if event.Signal != archivewire.RecordingSignal_STOP {
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

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
	cfg              Config
	proxy            *Proxy
	poll             func() int
	responsePoller   *ControlResponsePoller
	descriptorPoller *RecordingDescriptorPoller
	controlSessionID int64
}

func (client *Client) sessionControlOps(pollFn func() int) archiveControlOps {
	return archiveControlOps{
		aeronClient:      client.aeronClient,
		cfg:              client.cfg,
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
		ops.cfg,
		ops.poll,
		ops.responsePoller,
		func() (int64, bool) { return offer(correlationID) },
		ops.controlSessionID,
		correlationID,
	)
}

// startRecording offers a StartRecordingRequest2 and blocks until the archive replies
// with a matching ControlResponse. RelevantID semantics: see Client.StartRecording.
func (ops archiveControlOps) startRecording(
	ctx context.Context,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop archivewire.BooleanType,
	channel []byte,
) (int64, error) {
	fields, err := ops.offerAndAwait(ctx, func(correlationID int64) (int64, bool) {
		return ops.proxy.TryStartRecording(
			ops.controlSessionID,
			correlationID,
			streamID,
			sourceLocation,
			autoStop,
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

// truncateRecording offers a TruncateRecordingRequest and blocks until the
// archive replies with a matching ControlResponse. Returns RelevantID as the
// deleted segment count.
func (ops archiveControlOps) truncateRecording(
	ctx context.Context,
	recordingID int64,
	position int64,
) (int64, error) {
	fields, err := ops.offerAndAwait(ctx, func(correlationID int64) (int64, bool) {
		return ops.proxy.TryTruncateRecording(
			ops.controlSessionID,
			correlationID,
			recordingID,
			position,
		)
	})
	if err != nil {
		return 0, err
	}
	return fields.RelevantID, nil
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
		ops.cfg,
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

// offerAwaitRecordingSignal offers one control request and blocks until a
// latched recording signal completes the correlated wait.
//
// Behaviour:
//   - draws the correlation id and passes it to arm and every offer retry
//   - arms the response poller await; re-arms the await to 0 on return
//   - a matching OK ControlResponse alone does not complete the wait
//   - a latched signal completes the wait
//   - a matching ERROR fails the wait only before the signal latches
//
// arm may be nil when the tracker matches without correlation arming.
// Callers must Reset the tracker before calling.
func (ops archiveControlOps) offerAwaitRecordingSignal(
	ctx context.Context,
	targets ControlPollTargets,
	operation string,
	arm func(correlationID int64),
	offer func(correlationID int64) (int64, bool),
	latched func() bool,
) error {
	correlationID := ops.aeronClient.NextCorrelationID()
	if arm != nil {
		arm(correlationID)
	}
	if targets.ResponsePoller != nil {
		targets.ResponsePoller.ResetAwait(correlationID)
		defer targets.ResponsePoller.ArmAwait(0)
	}
	if err := offerControlRequest(
		ctx, ops.aeronClient, ops.cfg, ops.poll, func() (int64, bool) {
			return offer(correlationID)
		},
	); err != nil {
		return err
	}

	// Signal proof may already be latched before the await loop begins.
	return runControlAwaitLoop(
		ctx,
		ops.aeronClient,
		ops.cfg,
		ops.poll,
		ops.cfg.ControlResponseTimeout,
		correlationID,
		operation,
		func() (bool, error) {
			return recordingSignalProof(
				targets.ResponsePoller,
				latched(),
				ops.controlSessionID,
				correlationID,
			)
		},
	)
}

// startRecordingAwaitSignal implements [Client.StartRecordingAwaitSignal]; see that
// godoc for the START-vs-ControlResponse proof and timeout contract.
func (ops archiveControlOps) startRecordingAwaitSignal(
	ctx context.Context,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop archivewire.BooleanType,
	channel []byte,
	targets ControlPollTargets,
	tracker *StartSignalTracker,
) error {
	if tracker == nil {
		return errors.New("archive: start recording await signal: tracker must be non-nil")
	}
	tracker.Reset()
	return ops.offerAwaitRecordingSignal(
		ctx,
		targets,
		StartRecordingAwaitOperation,
		tracker.arm,
		func(correlationID int64) (int64, bool) {
			return ops.proxy.TryStartRecording(
				ops.controlSessionID,
				correlationID,
				streamID,
				sourceLocation,
				autoStop,
				channel,
			)
		},
		tracker.Started,
	)
}

// extendRecordingAwaitSignal implements [Client.ExtendRecordingAwaitSignal]; see that
// godoc for the EXTEND-vs-ControlResponse proof and timeout contract.
func (ops archiveControlOps) extendRecordingAwaitSignal(
	ctx context.Context,
	recordingID int64,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop archivewire.BooleanType,
	channel []byte,
	targets ControlPollTargets,
	tracker *ExtendSignalTracker,
) error {
	if tracker == nil {
		return errors.New("archive: extend recording await signal: tracker must be non-nil")
	}
	tracker.Reset()
	return ops.offerAwaitRecordingSignal(
		ctx,
		targets,
		ExtendRecordingAwaitOperation,
		tracker.arm,
		func(correlationID int64) (int64, bool) {
			return ops.proxy.TryExtendRecording(
				ops.controlSessionID,
				correlationID,
				recordingID,
				streamID,
				sourceLocation,
				autoStop,
				channel,
			)
		},
		tracker.Extended,
	)
}

// consumeMatchedControlResponse matches a latched ControlResponse for
// correlationID.
//
// Returns:
//   - matched true, nil: matching OK consumed. Capture cleared
//   - matched true, err: matching error ControlResponse. Capture stays latched
//   - matched false, nil: no match yet
func consumeMatchedControlResponse(
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (matched bool, err error) {
	_, mapped, ok := tryMatchControlResponseDiscardStale(
		poller,
		controlSessionID,
		correlationID,
	)
	if !ok {
		return false, nil
	}
	if mapped == nil {
		poller.ResetCapture()
	}
	return true, mapped
}

// consumeAwaitingControlResponse matches a latched ControlResponse for the
// armed await.
//
// Outcome:
//   - done true, err: matching error ControlResponse
//   - done false, nil + capture cleared: matching OK consumed; keep polling
//   - done false, nil: no match yet
func consumeAwaitingControlResponse(
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (done bool, err error) {
	matched, err := consumeMatchedControlResponse(
		poller,
		controlSessionID,
		correlationID,
	)
	if !matched {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return false, nil
}

// clearMatchingControlResponse drops a latched matching terminal so demux can
// accept later responses.
func clearMatchingControlResponse(
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) {
	if _, _, ok := TryMatchControlResponse(poller, controlSessionID, correlationID); ok {
		poller.ResetCapture()
	}
}

// recordingSignalProof reports whether the recording signal is latched, or fails
// on a matching error ControlResponse.
//
// A matching OK is consumed without completing. The await must keep polling for
// the recording signal.
//
// Returns:
//   - done true, nil: latched (matching ControlResponse cleared)
//   - done true, err: matching error ControlResponse and signal not latched
//   - done false, nil: keep polling
//
// When latched, clears a matching terminal so demux can accept later responses.
func recordingSignalProof(
	poller *ControlResponsePoller,
	latched bool,
	controlSessionID int64,
	correlationID int64,
) (done bool, err error) {
	if latched {
		clearMatchingControlResponse(poller, controlSessionID, correlationID)
		return true, nil
	}
	return consumeAwaitingControlResponse(poller, controlSessionID, correlationID)
}

// stopRecordingAwaitSignal implements [Client.StopRecordingAwaitSignal]; see that
// godoc for the STOP-vs-ControlResponse proof and timeout contract.
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

	// StopSignalTracker matches by recording id, not by correlation id. No arm.
	return ops.offerAwaitRecordingSignal(
		ctx,
		targets,
		StopRecordingAwaitOperation,
		nil,
		func(correlationID int64) (int64, bool) {
			return ops.proxy.TryStopRecording(
				ops.controlSessionID,
				correlationID,
				streamID,
				channel,
			)
		},
		tracker.Stopped,
	)
}

// StartSignalTracker records the START RecordingSignalEvent for one start request.
//
// The tracker implements RecordingSignalHandler for Client.StartRecordingAwaitSignal.
//
// Behaviour:
//   - records the first START whose correlation id matches the armed request
//   - ignores every other signal kind and every other correlation id
//   - an unarmed tracker matches nothing
//   - arm accepts any correlation id, including 0
//
// Concurrency: invoker-only with the poll that drives PollControlEvents.
//
// RecordingSignal runs on that polling goroutine and must not block.
type StartSignalTracker struct {
	controlSessionID   int64
	correlationID      int64
	armed              bool
	started            bool
	startedRecordingID int64
}

var _ RecordingSignalHandler = (*StartSignalTracker)(nil)

// NewStartSignalTracker returns a tracker matching START signals for controlSessionID.
//
// Zero matches all sessions, mirroring NewStopSignalTracker.
func NewStartSignalTracker(controlSessionID int64) *StartSignalTracker {
	return &StartSignalTracker{
		controlSessionID: controlSessionID,
	}
}

// Reset clears prior START capture and disarms the tracker.
//
// After Reset the tracker can capture a new start await.
func (t *StartSignalTracker) Reset() {
	t.correlationID = 0
	t.armed = false
	t.started = false
	t.startedRecordingID = 0
}

// arm sets the correlation id that RecordingSignal must match and marks the tracker
// armed. Correlation id 0 is a valid armed value.
func (t *StartSignalTracker) arm(correlationID int64) {
	t.correlationID = correlationID
	t.armed = true
}

// RecordingSignal records the first matching START signal.
func (t *StartSignalTracker) RecordingSignal(event codecs.RecordingSignalEventFields) {
	if event.Signal != archivewire.RecordingSignal_START {
		return
	}
	if !t.armed || event.CorrelationID != t.correlationID {
		return
	}
	if t.controlSessionID != 0 && event.ControlSessionID != t.controlSessionID {
		return
	}
	if t.started {
		return
	}
	t.started = true
	t.startedRecordingID = event.RecordingID
}

// Started reports whether a matching START signal was recorded.
func (t *StartSignalTracker) Started() bool {
	return t.started
}

// StartedRecordingID returns the catalog recording id from the recorded START signal,
// or 0 when none was recorded.
func (t *StartSignalTracker) StartedRecordingID() int64 {
	return t.startedRecordingID
}

// ExtendSignalTracker latches the EXTEND RecordingSignalEvent for one extend
// request.
//
// The tracker implements RecordingSignalHandler for
// Client.ExtendRecordingAwaitSignal.
//
// Behaviour:
//   - latches the first EXTEND whose correlation id and recording id both match
//     the armed request
//   - ignores every other signal kind, every other correlation id, and every
//     other recording
//   - an unarmed tracker matches nothing
//   - arm accepts any correlation id, including 0
//
// Concurrency: invoker-only with the poll that drives PollControlEvents.
//
// RecordingSignal:
//   - runs on that polling goroutine
//   - must not block
type ExtendSignalTracker struct {
	controlSessionID int64
	correlationID    int64
	armed            bool
	recordingID      int64
	extended         bool
}

var _ RecordingSignalHandler = (*ExtendSignalTracker)(nil)

// NewExtendSignalTracker returns a tracker matching EXTEND signals for
// controlSessionID and recordingID. A zero controlSessionID matches all
// sessions, mirroring NewStopSignalTracker. The returned tracker is unarmed.
func NewExtendSignalTracker(controlSessionID, recordingID int64) *ExtendSignalTracker {
	return &ExtendSignalTracker{
		controlSessionID: controlSessionID,
		recordingID:      recordingID,
	}
}

// Reset clears prior EXTEND capture and disarms the tracker.
//
// After Reset the tracker can capture a new extend await.
func (t *ExtendSignalTracker) Reset() {
	t.correlationID = 0
	t.armed = false
	t.extended = false
}

// arm sets the correlation id that RecordingSignal must match and marks the
// tracker armed. Correlation id 0 is a valid armed value.
func (t *ExtendSignalTracker) arm(correlationID int64) {
	t.correlationID = correlationID
	t.armed = true
}

// RecordingSignal latches the first matching EXTEND signal.
func (t *ExtendSignalTracker) RecordingSignal(event codecs.RecordingSignalEventFields) {
	if event.Signal != archivewire.RecordingSignal_EXTEND {
		return
	}
	if !t.armed || event.CorrelationID != t.correlationID {
		return
	}
	if t.controlSessionID != 0 && event.ControlSessionID != t.controlSessionID {
		return
	}
	if event.RecordingID != t.recordingID {
		return
	}
	t.extended = true
}

// Extended reports whether a matching EXTEND signal was recorded.
func (t *ExtendSignalTracker) Extended() bool {
	return t.extended
}

// ExtendedRecordingID returns the tracker's recording id after a matching
// EXTEND latches, or 0 when no EXTEND was recorded.
//
// RecordingSignal rejects every other recording id, so the returned id is
// always the id passed to NewExtendSignalTracker.
func (t *ExtendSignalTracker) ExtendedRecordingID() int64 {
	if t.extended {
		return t.recordingID
	}
	return 0
}

// StopSignalTracker records the STOP RecordingSignalEvent for a target control session
// and recording.
//
// The tracker implements RecordingSignalHandler for Client.StopRecordingAwaitSignal.
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

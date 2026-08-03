package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// recordingIDUnknown is the catalog id sentinel while a start or extend is in
// flight, or after a dirty attempt that never latched START or EXTEND.
const recordingIDUnknown int64 = -1

// autoStopExtend is the autoStop value every extend request carries.
//
//   - matches upstream
//   - archive releases the recording subscription after a refused extend that
//     already replied OK
//   - rollback stop also releases the subscription
//   - release does not depend on that stop succeeding
const autoStopExtend = true

// TermRecordingLookup reports the recording id every TERM entry in the local
// recording log shares.
//
// Behaviour:
//   - implementations return NullRecordingID when no TERM entry exists
//   - Null selects a fresh start recording
//   - any other id selects extend of the established recording
//
// Concurrency: invoker-only.
//
// Read on the same goroutine as [consensus.Agent.DoWork].
//
// Lifetime: the module owns the recording log and closes it after it clears the
// recording control. No read happens after that close.
type TermRecordingLookup interface {
	TermRecordingID() int64
}

var _ TermRecordingLookup = (*recordinglog.Log)(nil)

// logArchiveControl is the archive surface logRecording uses.
//
// *archive.Client implements it. Tests may substitute a double that records
// which stop path ran.
type logArchiveControl interface {
	ControlSessionID() int64
	StartRecordingAwaitSignal(
		ctx context.Context,
		streamID int32,
		sourceLocation archivewire.SourceLocation,
		channel []byte,
		tracker *archive.StartSignalTracker,
	) (recordingID int64, err error)
	ExtendRecordingAwaitSignal(
		ctx context.Context,
		recordingID int64,
		streamID int32,
		sourceLocation archivewire.SourceLocation,
		autoStop bool,
		channel []byte,
		tracker *archive.ExtendSignalTracker,
	) (extendedRecordingID int64, err error)
	StopRecording(ctx context.Context, streamID int32, channel []byte) error
	StopRecordingAwaitSignal(
		ctx context.Context,
		streamID int32,
		channel []byte,
		tracker *archive.StopSignalTracker,
	) error
	ListRecording(
		ctx context.Context,
		recordingID int64,
	) (codecs.RecordingDescriptorFields, error)
	TruncateRecording(
		ctx context.Context,
		recordingID int64,
		position int64,
	) (deletedSegmentCount int64, err error)
}

var _ logArchiveControl = (*archive.Client)(nil)

// logRecording starts, stops, and truncates this member's cluster log
// recording through the module's archive control client.
//
// Behaviour:
//   - builds a session-scoped recording channel from the topology log channel
//     and the supplied log session id
//   - when the recording log holds no TERM entry, starts a fresh recording and
//     awaits the correlated START signal for the catalog recording id
//   - when the recording log holds an established TERM recording id, extends
//     that recording and awaits the correlated EXTEND signal
//   - keeps the channel bytes and stream id the start or extend used so the
//     stop names the same recording
//   - offers truncate through the archive and stores the truncate position for
//     StopPositionFor; the caller must not call truncate while the recording is
//     active
//   - owns needs-stop state via startChannel: a dirty start keeps the channel
//     until a later stop drains and clears it, and a re-start drains any prior
//     unstopped channel first
//
// Concurrency: invoker-only.
//
// Every method blocks the invoker until the archive replies or
// ControlResponseTimeout expires.
//
// Lifetime:
//   - outlives the agent method calls that use the control
//   - module clears the control from the agent before closing the archive
//     client
//   - module clears the recording control from the agent before it closes the
//     recovery that owns the recording log
//   - no TermRecordingID read happens after that close
//   - retained context is Open's context with cancel detached
//     (context.WithoutCancel)
//   - Open cancel does not fail later start, stop, or truncate
//
// ControlResponseTimeout still bounds every await.
//
// Ownership:
//   - does not own the archive client
//   - does not own the topology
//   - does not own the term recording lookup
type logRecording struct {
	ctx          context.Context
	client       logArchiveControl
	terms        TermRecordingLookup
	logChannel   string
	logStreamID  int32
	startChannel []byte
	// recordingID is the live catalog id for the armed start, or
	// recordingIDUnknown while the start is unproven or after a dirty arm.
	recordingID int64

	// latchedRecordingID is the catalog id after the last clean stop or successful
	// truncate. StopPositionFor matches this id when nothing is pending. The id
	// survives a failed re-start that sets recordingID to unknown then drains
	// dirty.
	latchedRecordingID int64
	stopPosition       int64
}

var (
	_ consensus.LogRecordingControl = (*logRecording)(nil)
	_ consensus.LogTruncateControl  = (*logRecording)(nil)
)

// moduleControlContext returns the context module archive controls use for
// control calls.
//
// The returned context keeps Open values and detaches cancel with
// context.WithoutCancel so Open cancel does not fail later control calls. A
// nil input returns context.Background().
func moduleControlContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func newLogRecording(
	ctx context.Context,
	client *archive.Client,
	logChannel string,
	logStreamID int32,
	terms TermRecordingLookup,
) *logRecording {
	ctx = moduleControlContext(ctx)
	return &logRecording{
		ctx:                ctx,
		client:             client,
		terms:              terms,
		logChannel:         logChannel,
		logStreamID:        logStreamID,
		recordingID:        recordingIDUnknown,
		latchedRecordingID: recordingIDUnknown,
	}
}

// sessionScopedLogChannel appends session-id=<logSessionID> to logChannel.
func sessionScopedLogChannel(logChannel string, logSessionID int32) string {
	return aeron.AppendChannelParam(
		logChannel,
		aeron.ChannelSessionIDParam,
		strconv.FormatInt(int64(logSessionID), 10),
	)
}

// needsStop reports whether a prior start left a channel that must still be
// stopped.
func (r *logRecording) needsStop() bool {
	return len(r.startChannel) > 0
}

// IsActive reports whether a successful start or extend has not yet cleanly stopped.
//
//   - true only when a start channel is kept and the catalog id is known from
//     START or EXTEND
//   - dirty starts keep the channel with an unknown catalog id and are not
//     active
func (r *logRecording) IsActive() bool {
	return r.needsStop() && r.recordingID >= 0
}

// isUnknownSubscription reports whether err is archive ErrorUnknownSubscription.
func isUnknownSubscription(err error) bool {
	code, ok := archive.ExceptionCode(err)
	return ok && code == archive.ErrorUnknownSubscription
}

// establishedTermRecordingID returns the shared TERM recording id, or
// NullRecordingID when no lookup is wired or the log holds no TERM entry.
func (r *logRecording) establishedTermRecordingID() int64 {
	if r.terms == nil {
		return recordinglog.NullRecordingID
	}
	return r.terms.TermRecordingID()
}

// StartLogRecording starts or extends recording this member's cluster log and
// returns the catalog recording id.
//
// Behaviour:
//   - drains any prior unstopped channel first (dirty start or missed prepare)
//   - builds a session-scoped recording channel from logSessionID
//   - invalidates the catalog id until START or EXTEND proves
//   - when the recording log holds no TERM entry, offers a start request and
//     waits for the correlated START recording signal
//   - when the recording log holds an established TERM recording id, offers an
//     extend request for that id with autoStop true and waits for the correlated
//     EXTEND recording signal; the returned id is the established id
//   - on failure after the offer may have started, drains via StopLogRecording
//     (dirty: OK-bound StopRecording)
//   - treats ErrorUnknownSubscription as a clean rollback and clears
//     startChannel
//   - a dirty rollback keeps startChannel and leaves the catalog id unknown for
//     a later StopLogRecording or Close drain
//
// Does not refuse session ids, including -1.
//
// Structural refusal is Agent.JoinLogAsLeader only.
//
// Returns an error when:
//   - a prior unstopped recording cannot be drained
//   - the start or extend request cannot be offered within the offer bound
//   - the archive refuses the start or extend
//   - no correlated START or EXTEND signal arrives within the response bound
func (r *logRecording) StartLogRecording(
	sourceLocation archivewire.SourceLocation,
	logSessionID int32,
) (
	recordingID int64,
	err error,
) {
	if r.needsStop() {
		if drainErr := r.StopLogRecording(); drainErr != nil {
			return 0, fmt.Errorf(
				"cluster: drain prior log recording before start: %w",
				drainErr,
			)
		}
	}

	channelBytes := []byte(sessionScopedLogChannel(r.logChannel, logSessionID))
	// Arm channel state for this attempt.
	//
	// Catalog id stays unknown until START or EXTEND proves so a dirty rollback
	// cannot stop or list a stale or zero id.
	r.startChannel = channelBytes
	r.recordingID = recordingIDUnknown

	established := r.establishedTermRecordingID()
	var operation string
	if established == recordinglog.NullRecordingID {
		operation = "start log recording"
		tracker := archive.NewStartSignalTracker(r.client.ControlSessionID())
		recordingID, err = r.client.StartRecordingAwaitSignal(
			r.ctx,
			r.logStreamID,
			sourceLocation,
			channelBytes,
			tracker,
		)
	} else {
		operation = fmt.Sprintf("extend log recording %d", established)
		tracker := archive.NewExtendSignalTracker(
			r.client.ControlSessionID(),
			established,
		)
		recordingID, err = r.client.ExtendRecordingAwaitSignal(
			r.ctx,
			established,
			r.logStreamID,
			sourceLocation,
			autoStopExtend,
			channelBytes,
			tracker,
		)
	}
	if err != nil {
		// Armed state is the dirty stop path. Start and extend rollback share
		// StopLogRecording with the prepare/Close drain.
		if stopErr := r.StopLogRecording(); stopErr != nil {
			err = errors.Join(err, stopErr)
		}
		return 0, fmt.Errorf(
			"cluster: %s for stream %d on %s: %w",
			operation,
			r.logStreamID,
			string(channelBytes),
			err,
		)
	}

	r.recordingID = recordingID
	return recordingID, nil
}

// StopLogRecording stops the recording StartLogRecording started. A known
// catalog id stores the stop position for [logRecording.StopPositionFor].
//
// Behaviour:
//   - offers stop on the channel and stream the last armed start used
//   - known catalog id: awaits STOP and stores the stop position and catalog
//     id. On ErrorUnknownSubscription, one ListRecording resolves the stop
//     position
//   - unknown catalog id (dirty): StopRecording (OK-bound). Keeps the prior
//     stored position and stored catalog id. ErrorUnknownSubscription is
//     clean. Must not use a match-any STOP tracker (negative recording id)
//   - clean stop clears startChannel so a later call is a no-op
//   - second call with nothing started: offers nothing
//
// Idempotent.
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - the stop request cannot be offered within the offer bound
//   - known catalog id and no STOP signal within the response bound
//   - known catalog id, ErrorUnknownSubscription, and ListRecording cannot
//     resolve a stop position
//
// On error:
//   - the recording may still be running
//   - startChannel stays so a later stop can retry
func (r *logRecording) StopLogRecording() error {
	if !r.needsStop() {
		return nil
	}

	// Dirty: OK-bound stop. No stop position to adopt.
	if r.recordingID < 0 {
		err := r.client.StopRecording(r.ctx, r.logStreamID, r.startChannel)
		if err != nil && !isUnknownSubscription(err) {
			return err
		}
		r.markStopped(r.stopPosition)
		return nil
	}

	tracker := archive.NewStopSignalTracker(
		r.client.ControlSessionID(),
		r.recordingID,
	)
	err := r.client.StopRecordingAwaitSignal(
		r.ctx,
		r.logStreamID,
		r.startChannel,
		tracker,
	)
	if err != nil {
		if isUnknownSubscription(err) {
			return r.resolveStopPositionFromCatalog()
		}
		return err
	}

	r.markStopped(tracker.StoppedPosition())
	return nil
}

// markStopped records a clean stop. The method stores stopPosition and clears
// the channel so a later StopLogRecording is a no-op.
//
// When the live catalog id is known, markStopped also stores that id for
// StopPositionFor. Dirty drains leave the prior stored id unchanged.
func (r *logRecording) markStopped(stopPosition int64) {
	if r.recordingID >= 0 {
		r.latchedRecordingID = r.recordingID
	}
	r.stopPosition = stopPosition
	r.startChannel = nil
}

// catalogStopPosition reads the archive catalog stop position for recordingID.
//
// Behaviour:
//   - performs one bounded ListRecording
//   - does not latch
//
// The caller decides whether the value belongs to the recording this control
// stopped. A read for a foreign id cannot overwrite the retained position.
//
// Returns an error when:
//   - the catalog read fails
//   - the descriptor carries no stop position
func (r *logRecording) catalogStopPosition(recordingID int64) (int64, error) {
	desc, err := r.client.ListRecording(r.ctx, recordingID)
	if err != nil {
		return 0, fmt.Errorf(
			"cluster: resolve log recording stop position for recording %d: %w",
			recordingID,
			err,
		)
	}
	if desc.StopPosition == recordinglog.NullPosition {
		return 0, fmt.Errorf(
			"cluster: log recording %d reports no stop position",
			recordingID,
		)
	}
	return desc.StopPosition, nil
}

// resolveStopPositionFromCatalog reads the catalog stop position for the
// retained recording id after ErrorUnknownSubscription on stop. The method
// latches the position through markStopped.
func (r *logRecording) resolveStopPositionFromCatalog() error {
	stopPosition, err := r.catalogStopPosition(r.recordingID)
	if err != nil {
		return err
	}
	r.markStopped(stopPosition)
	return nil
}

// StopPositionFor returns the archive stop position recorded for recordingID.
//
// Behaviour:
//   - same id as the last clean stop or successful truncate (stored catalog
//     id) and nothing pending: returns the stored stop position. No catalog
//     read. A later clean stop overwrites a truncate store.
//   - any other case: reads the catalog once. Does not store that read.
//   - same id while stop still pending: takes the catalog path. Errors only
//     when the catalog read fails or reports no stop position
//
// StopPositionFor has no caller-supplied position to fall back to.
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - recordingID is negative
//   - the catalog read fails
//   - the catalog reports no stop position for recordingID
//
// On error the caller must not substitute another position.
func (r *logRecording) StopPositionFor(recordingID int64) (int64, error) {
	if recordingID < 0 {
		return recordinglog.NullPosition, fmt.Errorf(
			"cluster: resolve log recording stop position for recording %d: negative recording id",
			recordingID,
		)
	}
	if recordingID == r.latchedRecordingID && r.latchedRecordingID >= 0 && !r.needsStop() {
		return r.stopPosition, nil
	}
	stopPosition, err := r.catalogStopPosition(recordingID)
	if err != nil {
		return recordinglog.NullPosition, err
	}
	return stopPosition, nil
}

// TruncateLogRecording removes every recorded byte at or above position.
//
// Behaviour: offers one bounded archive truncate for recordingID. On success
// markTruncated stores recordingID and position, so
// [logRecording.StopPositionFor] reports the truncate position until a later
// clean stop overwrites it. On error nothing is stored.
//
// Concurrency: invoker-only. Blocks the invoker until the archive replies or
// ControlResponseTimeout expires.
//
// Returns deletedSegmentCount from the archive reply with a nil error on
// success. The error wraps the archive cause. deletedSegmentCount is
// meaningless on error.
func (r *logRecording) TruncateLogRecording(
	recordingID int64,
	position int64,
) (
	deletedSegmentCount int64,
	err error,
) {
	count, err := r.client.TruncateRecording(r.ctx, recordingID, position)
	if err != nil {
		return 0, fmt.Errorf(
			"cluster: truncate log recording %d to %d: %w",
			recordingID, position, err,
		)
	}
	r.markTruncated(recordingID, position)
	return count, nil
}

// markTruncated stores recordingID and the truncate position for
// StopPositionFor after a successful archive truncate.
//
// startChannel is kept. A truncate may run while a dirty drain is pending, and
// that drain still needs the channel. While the channel is pending,
// StopPositionFor takes the catalog path, which reports the same lowered stop.
func (r *logRecording) markTruncated(recordingID, position int64) {
	r.latchedRecordingID = recordingID
	r.stopPosition = position
}

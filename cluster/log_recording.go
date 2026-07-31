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
}

var _ logArchiveControl = (*archive.Client)(nil)

// logRecording starts and stops this member's cluster log recording through the
// module's archive control client.
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
//   - Open cancel does not fail later start or stop
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
	recordingID  int64
	stopPosition int64
}

var _ consensus.LogRecordingControl = (*logRecording)(nil)

func newLogRecording(
	ctx context.Context,
	client *archive.Client,
	logChannel string,
	logStreamID int32,
	terms TermRecordingLookup,
) *logRecording {
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	return &logRecording{
		ctx:         ctx,
		client:      client,
		terms:       terms,
		logChannel:  logChannel,
		logStreamID: logStreamID,
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
		if _, drainErr := r.StopLogRecording(); drainErr != nil {
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
		if _, stopErr := r.StopLogRecording(); stopErr != nil {
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

// StopLogRecording stops the recording StartLogRecording started and returns
// its stop position.
//
// Behaviour:
//   - offers stop on the channel and stream the last armed start used
//   - known catalog id: awaits STOP and adopts the latched stop position. On
//     ErrorUnknownSubscription, one ListRecording resolves the stop position
//   - unknown catalog id (dirty): StopRecording (OK-bound). Keeps the prior
//     stopPosition. ErrorUnknownSubscription is clean. Must not use a
//     match-any STOP tracker (negative recording id)
//   - clean stop clears startChannel so a later call is a no-op
//
// Idempotent.
//
// A second call with nothing started returns the position the previous stop
// reported, and offers nothing.
//
// Returns an error when:
//   - the stop request cannot be offered within the offer bound
//   - known catalog id and no STOP signal within the response bound
//   - known catalog id, ErrorUnknownSubscription, and ListRecording cannot
//     resolve a stop position
//
// On error:
//   - the recording may still be running
//   - the caller must not treat the returned position as valid
//   - startChannel stays so a later stop can retry
func (r *logRecording) StopLogRecording() (
	stopPosition int64,
	err error,
) {
	if !r.needsStop() {
		return r.stopPosition, nil
	}

	// Dirty: OK-bound stop. No stop position to adopt.
	if r.recordingID < 0 {
		err = r.client.StopRecording(r.ctx, r.logStreamID, r.startChannel)
		if err != nil && !isUnknownSubscription(err) {
			return 0, err
		}
		r.markStopped(r.stopPosition)
		return r.stopPosition, nil
	}

	tracker := archive.NewStopSignalTracker(
		r.client.ControlSessionID(),
		r.recordingID,
	)
	err = r.client.StopRecordingAwaitSignal(
		r.ctx,
		r.logStreamID,
		r.startChannel,
		tracker,
	)
	if err != nil {
		if isUnknownSubscription(err) {
			return r.resolveStopPositionFromCatalog()
		}
		return 0, err
	}

	r.markStopped(tracker.StoppedPosition())
	return r.stopPosition, nil
}

// markStopped records a clean stop: retains position, clears the channel so a
// later StopLogRecording is a no-op.
func (r *logRecording) markStopped(stopPosition int64) {
	r.stopPosition = stopPosition
	r.startChannel = nil
}

// resolveStopPositionFromCatalog reads the catalog stop position for the
// retained recording id after ErrorUnknownSubscription on stop.
func (r *logRecording) resolveStopPositionFromCatalog() (int64, error) {
	desc, err := r.client.ListRecording(r.ctx, r.recordingID)
	if err != nil {
		return 0, fmt.Errorf(
			"cluster: resolve log recording stop position for recording %d: %w",
			r.recordingID,
			err,
		)
	}
	if desc.StopPosition == recordinglog.NullPosition {
		return 0, fmt.Errorf(
			"cluster: log recording %d reports no stop position",
			r.recordingID,
		)
	}
	r.markStopped(desc.StopPosition)
	return r.stopPosition, nil
}

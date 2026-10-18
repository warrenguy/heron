package consensus

import "github.com/warrenguy/heron/archive/codecs/archivewire"

// LogRecordingControl starts, extends, or stops this member's cluster log
// recording. The control also resolves stop positions for prepare and
// reports the STOP signal of the current recording.
//
// Concurrency: invoker-only. StartLogRecording, StopLogRecording,
// PollStopLogRecording, StopPositionFor, and ConsumeStopSignal run on the
// [Agent.DoWork] goroutine. StartLogRecording, StopLogRecording, and
// StopPositionFor block until the archive replies or the deadline expires.
// PollStopLogRecording, IsActive, and ConsumeStopSignal never block.
//
// An implementation must:
//   - cap every wait on StartLogRecording, StopLogRecording, and
//     StopPositionFor
//   - drive the transport client while those methods wait
//
// Lifetime: the implementation outlives the [Agent] method calls that use
// the control. The module wires the control after the agent is built and
// clears the control before the archive client closes.
//
// Ownership: implementation does not own the [Agent] or a transport handle.
// The module owns the archive control client and closes it.
type LogRecordingControl interface {
	// StartLogRecording starts or extends recording this member's cluster log
	// and returns the catalog recording id from the proof signal, not the
	// subscription registration id.
	//
	// sourceLocation must be:
	//   - archivewire.SourceLocation_LOCAL for a leader recording its own log
	//     publication
	//   - archivewire.SourceLocation_REMOTE for a follower recording the
	//     leader's log publication
	//
	// logSessionID must be the cluster log publication session id the recording
	// filters on:
	//   - leader: own log publication session id
	//   - follower: leader's session id
	//
	// recordingChannel is the base channel URI for the recording, without a
	// session-id param. Empty uses the control's configured live log channel.
	// Catchup joins pass the catchup destination channel. Extend then sees the
	// catchup image join position, not a late-join live image.
	//
	// Concurrency: invoker-only. Blocks the invoker until the START or EXTEND
	// signal arrives or the bound expires.
	//
	// Returns an error when:
	//   - a prior unstopped recording cannot be drained
	//   - the start or extend request cannot be offered within the offer bound
	//   - the archive refuses the start or extend
	//   - no correlated START or EXTEND signal arrives within the response bound
	//
	// On error:
	//   - the implementation has already attempted a bounded stop for the
	//     channel and stream it offered
	//   - a clean rollback leaves no archive recording subscription
	//   - a dirty rollback keeps the start channel so a later StopLogRecording
	//     or module Close can still offer stop
	//   - the returned id is meaningless
	StartLogRecording(
		sourceLocation archivewire.SourceLocation,
		logSessionID int32,
		recordingChannel string,
	) (
		recordingID int64,
		err error,
	)

	// StopLogRecording stops the recording StartLogRecording started. A known
	// catalog id stores the stop position for StopPositionFor.
	//
	// Abandons a pending PollStopLogRecording attempt before the blocking
	// drain. Idempotent.
	//
	// Concurrency: invoker-only. Blocks the invoker until the stop proof arrives
	// or the bound expires.
	//
	// Returns an error when:
	//   - the stop request cannot be offered within the offer bound
	//   - known catalog id and no STOP signal within the response bound
	//   - known catalog id and catalog read cannot resolve a stop position
	//
	// On error the recording may still be running.
	StopLogRecording() error

	// PollStopLogRecording advances one pending stop of the recording
	// StartLogRecording started. nowNs is the duty-cycle deadline clock.
	//
	// Never blocks. Never calls DoWork. Never calls archive Poll.
	//
	// [Agent.PrepareForNewLeadership] uses this poll. StopLogRecording stays
	// the blocking drain for module Close and start-time rollback.
	//
	// Concurrency: invoker-only.
	//
	// Returns:
	//   - true, nil: stop confirmed, or nothing needed stop
	//   - false, nil: poll again
	//   - false, err: fail closed. A later poll starts a fresh attempt
	//
	// On error the recording may still be running.
	PollStopLogRecording(nowNs int64) (
		done bool,
		err error,
	)

	// IsActive reports whether a successful start or extend has not yet cleanly
	// stopped.
	//
	// Behaviour:
	//   - false before any successful start or extend
	//   - false after a dirty attempt (needs stop, catalog id still unknown)
	//   - true after StartLogRecording returns success until a clean stop
	//   - false after a clean stop
	//   - true when stop failed after a live start or extend so a later stop
	//     can retry
	//
	// Concurrency: invoker-only.
	IsActive() bool

	// ConsumeStopSignal takes the STOP mark. The archive STOP signal for the
	// live recording sets the mark.
	//
	// Never blocks. Never calls archive Poll.
	//
	// Concurrency: invoker-only.
	//
	// Returns:
	//   - stopPosition, true: a STOP arrived since the last call
	//   - recordinglog.NullPosition, false: no STOP arrived
	ConsumeStopSignal() (
		stopPosition int64,
		stopped bool,
	)

	// StopPositionFor returns the archive stop position recorded for
	// recordingID.
	//
	// Behaviour:
	//   - same id as the control after a clean stop or a successful truncate,
	//     and nothing pending: returns the stored stop position. No catalog
	//     read. A later clean stop overwrites a truncate store.
	//   - any other case: reads the archive catalog once. Does not store that
	//     read.
	//   - same id while stop still pending: takes the catalog path. Errors only
	//     when the catalog read fails or reports no stop position.
	//
	// recordingID must be a non-negative catalog recording id.
	//
	// Concurrency: invoker-only. Blocks the invoker for at most one bounded
	// catalog read.
	//
	// Returns an error when:
	//   - recordingID is negative
	//   - the catalog read fails
	//   - the catalog reports no stop position for recordingID
	//
	// On error the caller must not substitute another position.
	StopPositionFor(recordingID int64) (
		stopPosition int64,
		err error,
	)
}

// SetLogRecordingControl wires or clears the cluster log recording control used
// by [Agent.JoinLogAsLeader], [Agent.JoinLogAsFollower],
// [Agent.PrepareForNewLeadership], and the [Agent.DoWork] log stop check.
//
// Concurrency: invoker-only. Must not run concurrent with [Agent.DoWork].
//
// Nil is allowed. The three join and prepare methods then skip recording.
//
// Nil control outcomes:
//   - [Agent.JoinLogAsLeader] and [Agent.JoinLogAsFollower] return no error
//   - [Agent.IsRecordingActive] is false
//   - [Agent.PrepareForNewLeadership] skips stop and returns logPosition
//     unchanged with prepared true
//   - [Agent.DoWork] enters no election from a recording STOP
//
// A non-nil control makes [Agent.PrepareForNewLeadership] call
// PollStopLogRecording so a dirty start or extend still drains.
//
// Clearing with nil makes [Agent.IsRecordingActive] false. The agent has no
// control to query.
func (a *Agent) SetLogRecordingControl(control LogRecordingControl) {
	a.recordingControl = control
}

// LogRecordingControl returns the wired [LogRecordingControl], or nil if unset.
func (a *Agent) LogRecordingControl() LogRecordingControl {
	return a.recordingControl
}

package consensus

import "github.com/warrenguy/heron/archive/codecs/archivewire"

// LogRecordingControl starts, extends, or stops this member's cluster log
// recording. The control also resolves stop positions for prepare.
//
// Behaviour:
//   - keeps the recording channel and recording stream id
//   - keeps the catalog recording id after a successful start or extend
//   - on a clean stop, stores the stop position and catalog id for
//     StopPositionFor
//   - uses the archive control client
//   - builds the channel from the member's log channel and the supplied log
//     session id
//   - StopLogRecording stops the recording last started or extended by
//     StartLogRecording on the same channel and stream
//
// Concurrency: invoker-only. StartLogRecording, StopLogRecording, and
// StopPositionFor run on the [Agent.DoWork] goroutine. Those three methods
// block until the archive replies or the deadline expires. IsActive is
// invoker-only and non-blocking.
//
// An implementation must:
//   - cap every wait on StartLogRecording, StopLogRecording, and
//     StopPositionFor
//   - drive the transport client while those methods wait
//
// Lifetime: the implementation outlives the [Agent] method calls that use
// it. The module wires the control after the agent is built and clears the
// control before the archive client closes.
//
// Ownership:
//   - implementation does not own the [Agent]
//   - implementation owns no transport handle
//   - module owns the archive control client and closes it
type LogRecordingControl interface {
	// StartLogRecording starts or extends recording this member's cluster log
	// and returns the catalog recording id.
	//
	// Behaviour:
	//   - builds a session-scoped recording channel from logSessionID
	//   - when no established TERM recording id exists, offers a start request
	//     and waits for the correlated START recording signal
	//   - when an established TERM recording id exists, offers an extend
	//     request for that id and waits for the correlated EXTEND recording
	//     signal
	//   - returns the catalog recording id from that proof signal, not the
	//     subscription registration id the control response carries
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
	// Concurrency: invoker-only.
	//
	// Blocks the invoker until the START or EXTEND signal arrives or the bound
	// expires.
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
	) (
		recordingID int64,
		err error,
	)

	// StopLogRecording stops the recording StartLogRecording started. A known
	// catalog id stores the stop position for StopPositionFor.
	//
	// Behaviour:
	//   - offers stop on the channel and stream the last armed start used
	//   - known catalog id: stores the archive stop position from STOP
	//   - unknown catalog id (dirty): OK-bound drain only; keeps the prior stored
	//     position
	//   - second call with nothing started: offers nothing
	//
	// Idempotent.
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

	// StopPositionFor returns the archive stop position recorded for
	// recordingID.
	//
	// Behaviour:
	//   - same id as the control after a clean stop and nothing pending: returns
	//     the stored stop position. No catalog read.
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
// by [Agent.JoinLogAsLeader], [Agent.JoinLogAsFollower], and
// [Agent.PrepareForNewLeadership].
//
// Concurrency: invoker-only. Must not run concurrent with [Agent.DoWork].
//
// Nil is allowed. All three methods then skip recording.
//
// Nil control outcomes:
//   - [Agent.JoinLogAsLeader] and [Agent.JoinLogAsFollower] return no error
//   - [Agent.IsRecordingActive] is false
//   - [Agent.PrepareForNewLeadership] skips stop and returns logPosition
//     unchanged
//
// A non-nil control makes [Agent.PrepareForNewLeadership] call
// StopLogRecording on every prepare so a dirty start or extend still drains.
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

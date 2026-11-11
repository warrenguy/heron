package consensus

import "github.com/warrenguy/luas/archive/codecs/archivewire"

// LogRecordingControl starts, extends, or stops this member's cluster log
// recording. The control also resolves stop positions for prepare and
// reports the STOP signal of the current recording.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe
// from other goroutines. PollStopLogRecording, IsActive, and
// ConsumeStopSignal never block.
//
// An implementation must:
//   - cap every wait on StartLogRecording, StopLogRecording, and
//     StopPositionFor
//   - drive the transport client while those methods wait
//
// Lifetime: the implementation outlives the [Agent] method calls that use
// the control. The module wires the control after it builds the agent. The
// module clears the control before the archive client closes.
//
// Ownership: the implementation does not own the [Agent] or a transport handle.
// The module owns the archive control client and closes it.
type LogRecordingControl interface {
	// StartLogRecording starts or extends recording this member's cluster log.
	// Returns the catalog recording id from the proof signal, not the
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
	// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe
	// from other goroutines. Blocks until the START or EXTEND signal arrives,
	// the bound expires, the transport client terminates, or the archive
	// disconnects.
	//
	// Returns an error when:
	//   - the control cannot drain a prior unstopped recording
	//   - the control cannot offer the start or extend request within the offer
	//     bound
	//   - the archive refuses the start or extend
	//   - a correlated START or EXTEND signal does not arrive within the response
	//     bound
	//   - the transport client terminates. The error matches
	//     archive.ErrTransportTerminating
	//   - the archive disconnects after the offer. The error matches
	//     archive.ErrNotConnected
	//
	// On error:
	//   - the implementation has attempted a bounded stop for the channel and
	//     stream it offered
	//   - a clean rollback does not leave an archive recording subscription
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

	// StopLogRecording stops the recording StartLogRecording started. With a
	// known catalog id, the control stores the stop position for StopPositionFor.
	//
	// Abandons a pending PollStopLogRecording attempt before the blocking
	// drain. Idempotent.
	//
	// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe
	// from other goroutines. Blocks until the stop proof arrives, the bound
	// expires, the transport client terminates, or the archive disconnects.
	//
	// Returns an error when:
	//   - the control cannot offer the stop request within the offer bound
	//   - known catalog id and the STOP signal does not arrive within the
	//     response bound
	//   - known catalog id and the catalog read cannot resolve a stop position
	//   - the transport client terminates. The error matches
	//     archive.ErrTransportTerminating
	//   - the archive disconnects after the offer. The error matches
	//     archive.ErrNotConnected
	//
	// On error the recording may still be running.
	StopLogRecording() error

	// PollStopLogRecording advances one pending stop. The stop targets the
	// recording StartLogRecording started, or a recovered term recording by
	// identity. nowNs is the duty-cycle deadline clock.
	//
	// The identity stop runs only when the control does not need to stop a start
	// channel and does not store a stop for the term recording. The poll then
	// stores the catalog stop for that id.
	//
	// Never blocks. Never calls DoWork. Never calls archive Poll.
	//
	// [Agent.PrepareForNewLeadership] uses this poll. StopLogRecording is the
	// blocking drain for module Close and start-time rollback.
	//
	// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe
	// from other goroutines.
	//
	// Returns:
	//   - true, nil: stop confirmed, identity catalog stop stored, or the control
	//     does not need a stop
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
	// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe
	// from other goroutines.
	IsActive() bool

	// ConsumeStopSignal takes the STOP mark. The archive STOP signal for the
	// live recording sets the mark. A blocking stop does not set the mark.
	//
	// Never blocks. Never calls archive Poll.
	//
	// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe
	// from other goroutines.
	//
	// Returns:
	//   - stopPosition, true: a STOP arrived since the last call
	//   - recordinglog.NullPosition, false: a STOP did not arrive
	ConsumeStopSignal() (
		stopPosition int64,
		stopped bool,
	)

	// StopPositionFor returns the archive stop position recorded for
	// recordingID.
	//
	// Behaviour:
	//   - same id as the stored catalog id and a stop is not pending: returns
	//     the stored stop position. Does not read the catalog. A clean stop, a
	//     successful truncate, an identity catalog stop, or a plan seed stores
	//     the id. A later clean stop overwrites a truncate store.
	//   - any other case: reads the archive catalog once. Does not store that
	//     read.
	//
	// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe
	// from other goroutines. Blocks for at most one bounded catalog read.
	//
	// Returns an error when:
	//   - recordingID is negative
	//   - the catalog read fails
	//   - the catalog does not report a stop position for recordingID
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
// PollStopLogRecording so a dirty start or extend still drains. The poll also
// stops a recovered term recording by identity.
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

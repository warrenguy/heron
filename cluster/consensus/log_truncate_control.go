package consensus

// LogTruncateControl truncates this member's cluster log recording.
//
// Behaviour: an implementation uses the archive control client. It removes
// every recorded byte at or above position and lowers the recording's stop
// position to position. It does not stop a running recording; the caller must
// not call it while one is running.
//
// Concurrency: invoker-only. The method runs on the same goroutine as
// [Agent.DoWork] and blocks that goroutine until the archive replies or the
// bound expires. An implementation must bound the wait.
//
// Lifetime: an implementation outlives the [Agent] method calls that use it.
// The module wires it after the agent is built and clears it before the archive
// client closes.
//
// Ownership: an implementation does not own the [Agent]. The module owns the
// archive control client and closes it.
type LogTruncateControl interface {
	// TruncateLogRecording removes every recorded byte at or above position.
	//
	// Behaviour: offers a truncate request for recordingID and waits for the
	// control reply. position must be aligned to the log frame alignment and must
	// lie between the recording start position and its stop position. On success
	// the implementation must report position from
	// [LogRecordingControl.StopPositionFor] for recordingID until a later clean
	// stop overwrites it. [Agent.PrepareForNewLeadership] derives that stop and
	// must not re-raise above it.
	//
	// Concurrency: invoker-only. Blocks the invoker until the reply arrives or the
	// bound expires.
	//
	// Returns deletedSegmentCount, the number of segment files the archive queued
	// for deletion, with a nil error on success. Zero is a valid count: a truncate
	// to the current stop position removes nothing. deletedSegmentCount is
	// meaningless on error.
	//
	// Returns an error when the offer fails, the wait expires, or the archive
	// refuses. The error wraps the archive cause. On error nothing is latched for
	// StopPositionFor. Refuse codes and the data-header partial-update case live
	// on archive.Client.TruncateRecording.
	TruncateLogRecording(
		recordingID int64,
		position int64,
	) (
		deletedSegmentCount int64,
		err error,
	)
}

// SetLogTruncateControl wires or clears the cluster log truncate control used by
// [Agent.TruncateLogEntry].
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Nil is not allowed on the product path. [Agent.TruncateLogEntry] returns an
// error when no control is wired, because a silent skip would let the election
// lower a position the archive still holds.
func (a *Agent) SetLogTruncateControl(control LogTruncateControl) {
	a.truncateControl = control
}

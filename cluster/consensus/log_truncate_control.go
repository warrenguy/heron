package consensus

// LogTruncateControl truncates this member's cluster log recording.
//
// Behaviour: an implementation does not stop a running recording. The caller
// must not truncate a running recording.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
//
// Lifetime: an implementation outlives the [Agent] method calls that use it.
// The module wires it after the module builds the agent. The module clears it
// before the archive client closes.
//
// Ownership: an implementation does not own the [Agent]. The module owns the
// archive control client and closes it.
type LogTruncateControl interface {
	// TruncateLogRecording removes every recorded byte at or above position.
	//
	// Behaviour: offers a truncate request for recordingID. Waits for the control
	// reply. position must align to the log frame alignment. position must lie
	// between the recording start position and its stop position. On success the
	// implementation must report position from
	// [LogRecordingControl.StopPositionFor] for recordingID until a later clean
	// stop overwrites it. [Agent.PrepareForNewLeadership] derives that stop.
	// [Agent.PrepareForNewLeadership] must not re-raise above it.
	//
	// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe
	// from other goroutines. Blocks until the reply arrives, the deadline
	// expires, or the transport client terminates. An implementation must bound
	// the wait.
	//
	// Returns deletedSegmentCount, the number of segment files the archive queued
	// for deletion, with a nil error on success. Zero is a valid count. A
	// truncate to the current stop position does not remove a segment file.
	// deletedSegmentCount is meaningless on error.
	//
	// The error is non-nil when the offer fails, the deadline expires, the archive
	// refuses, or the transport client terminates. The error wraps the archive
	// cause. A transport fault matches archive.ErrTransportTerminating. On error
	// the implementation does not store a position for StopPositionFor. Refuse
	// codes and the data-header partial-update case live on
	// archive.Client.TruncateRecording.
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
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
//
// Nil is not allowed. [Agent.TruncateLogEntry] returns an error when no control
// is wired. A silent skip would let the election lower a position the archive
// still holds.
func (a *Agent) SetLogTruncateControl(control LogTruncateControl) {
	a.truncateControl = control
}

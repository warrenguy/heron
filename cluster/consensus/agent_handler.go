package consensus

// FollowerLogJoinParams carries the aeron.Image inputs to a follower log join,
// mirroring ConsensusModuleAgent.tryJoinLogAsFollower. The consensus module agent
// maps an aeron.Image to these fields at the call site.
type FollowerLogJoinParams struct {
	LogChannel      string
	LogStreamID     int32
	LogSessionID    int32
	LogJoinPosition int64
	IsLeaderStartup bool

	// IsBoundRejoin marks a re-join for the image the adapter already holds. The
	// join restarts the recording only. No image selection runs. The election
	// bound path sets the flag. Every other join leaves it false.
	IsBoundRejoin bool
}

// AgentHandler is the election-to-agent boundary for term transitions.
// Implemented by the consensus module agent; mocked in election tests.
//
// Three methods reach the archive control client through [LogRecordingControl]
// and can fail. A non-nil error from any of those methods means the caller
// must not proceed with the transition the caller asked for. The election
// routes every failure to its own error handler. The error handler returns
// the state machine to init and re-attempts on the next duty cycle.
//
// Method semantics follow aeron ConsensusModuleAgent.
type AgentHandler interface {
	// PrepareForNewLeadership resets election leadership state, then polls the
	// cluster log recording stop when a control is wired. Does not reset
	// commit.
	//
	// First call of an episode runs teardown. Later calls poll only.
	//
	// logPosition is the committed log position the election starts from. The
	// teardown rolls back each TimerEvent and SessionCloseEvent this member
	// appended above logPosition. The teardown also drops each session opened
	// above logPosition and every pre-open session.
	//
	// nowNs is the deadline clock for [LogRecordingControl.PollStopLogRecording].
	//
	// Returns:
	//   - lastAppendPosition, prepared true, nil: stop confirmed
	//   - NullPosition, prepared false, nil: poll again
	//   - NullPosition, prepared false, err: fail closed
	//
	// Caller must not adopt lastAppendPosition unless prepared is true.
	PrepareForNewLeadership(
		logPosition int64,
		nowNs int64,
	) (
		lastAppendPosition int64,
		prepared bool,
		err error,
	)

	// IsPrepareInProgress reports whether a PrepareForNewLeadership episode is
	// open. Open means the last PrepareForNewLeadership returned prepared false
	// with a nil error. The election reports zero work on a re-poll while the
	// episode is open.
	IsPrepareInProgress() bool

	// JoinLogAsLeader adopts the leadership term and starts the LOCAL cluster
	// log recording for logSessionID when a control is wired.
	//
	// Role, term id, term base, and leader-member clear always write first.
	//
	// A failed start leaves those fields set and returns an error; the
	// recording is not started. The caller must not write a recording-log
	// TERM entry and must route the error to its fail-closed handler.
	//
	// Returns an error when:
	//   - a control is wired, the recording is not already active, and the log
	//     publication is unbound
	//   - the start fails
	//
	// Nil control skips recording and returns nil.
	JoinLogAsLeader(
		leadershipTermID int64,
		logPosition int64,
		logSessionID int32,
		isStartup bool,
	) error

	// JoinLogAsFollower binds the live cluster log image for
	// params.LogSessionID and may start the REMOTE cluster log recording.
	//
	// params.IsBoundRejoin true skips image selection. The join restarts the
	// recording for the image the adapter already holds. Joined false then means
	// the bound image is not open on params.LogSessionID.
	//
	// Returns:
	//   - joined true, nil error: the image is bound. Recording starts only when a
	//     control is wired and no recording is already active; otherwise the join
	//     still succeeds without a new start.
	//   - joined false, nil error: the image is not available yet. The caller
	//     retries that case.
	//   - joined false, non-nil error: the recording cannot be started or a live
	//     recording cannot be stopped after a failed bind. The caller must not
	//     retry that case within this election attempt.
	JoinLogAsFollower(
		params FollowerLogJoinParams,
		nowNs int64,
	) (
		joined bool,
		err error,
	)

	ElectionComplete(nowNs int64)
}

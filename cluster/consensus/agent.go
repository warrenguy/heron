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
}

// AgentHandler is the election-to-agent boundary for term transitions.
//
// Implemented by the consensus module agent; mocked in election tests.
//
// Three methods reach the archive control client through
// [LogRecordingControl] and can fail.
//
// A non-nil error from any of those methods means the caller must not proceed
// with the transition the caller asked for.
//
// The election routes every failure to its own error handler.
//
// The error handler returns the state machine to init and re-attempts on the
// next duty cycle.
//
// Method semantics follow aeron ConsensusModuleAgent.
//
// PrepareForNewLeadership:
//   - resets role to FOLLOWER
//   - clears the known leader
//   - resets follower AppendPosition cadence fields
//   - keeps commit
//   - stops the cluster log recording when a control is wired
//   - returns the last-append position
//
// Log and ingress tear-down is deferred until full agent wiring.
//
// JoinLogAsLeader:
//   - adopts the leadership term
//   - starts the LOCAL cluster log recording when a control is wired
//
// JoinLogAsFollower (tryJoinLogAsFollower):
//   - binds the live log image
//   - may start the REMOTE cluster log recording
//   - returns true when the image is bound
//
// ElectionComplete:
//   - clears the election runner
//   - stamps timeOfLastLogUpdateNs (settled log-update clock) for the next
//     quiet-tick timeout window
type AgentHandler interface {
	// PrepareForNewLeadership stops the cluster log recording when a control
	// is wired and returns the last appended log position.
	//
	// Role, known leader, and follower append cadence fields always reset
	// before any stop attempt.
	//
	// Commit is kept.
	//
	// Stop runs for any wired control so a dirty start that never became
	// active still drains.
	//
	// Returns:
	//   - lastAppendPosition, nil: a live recording stopped (archive stop
	//     position)
	//   - logPosition, nil: no live recording, control is nil, or stop was a
	//     clean no-op (logPosition may be the null-position sentinel)
	//   - null-position sentinel, err: stop failed; when the recording was
	//     active the control stays active so a later prepare retries;
	//     lastAppendPosition must not be adopted
	PrepareForNewLeadership(
		logPosition int64,
		nowNs int64,
	) (
		lastAppendPosition int64,
		err error,
	)

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
	// Returns:
	//   - joined true, nil error: the image is bound.
	//     Recording starts only when a control is wired and no recording is
	//     already active; otherwise the join still succeeds without a new start.
	//   - joined false, nil error: the image is not available yet.
	//     The caller retries that case.
	//   - joined false, non-nil error: the recording cannot be started or a live
	//     recording cannot be stopped after a failed bind.
	//     The caller must not retry that case within this election attempt.
	JoinLogAsFollower(
		params FollowerLogJoinParams,
		nowNs int64,
	) (
		joined bool,
		err error,
	)

	ElectionComplete(nowNs int64)
}

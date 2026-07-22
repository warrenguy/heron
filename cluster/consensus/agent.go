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
// Implemented by the consensus module agent; mocked in election tests.
//
// Method semantics follow aeron ConsensusModuleAgent:
//   - PrepareForNewLeadership resets role to FOLLOWER, clears the known leader,
//     resets follower AppendPosition cadence fields, keeps commit, returns
//     lastAppendPosition (int64). Log/ingress tear-down is deferred until full
//     agent wiring.
//   - JoinLogAsLeader blocks until append position counter exists and services ready.
//   - JoinLogAsFollower (tryJoinLogAsFollower) returns true when the join succeeded.
//   - ElectionComplete finalises term, activates toggles, clears the election field.
type AgentHandler interface {
	PrepareForNewLeadership(logPosition, nowNs int64) (lastAppendPosition int64)
	JoinLogAsLeader(leadershipTermID, logPosition int64, logSessionID int32, isStartup bool)
	JoinLogAsFollower(params FollowerLogJoinParams, nowNs int64) (joined bool)
	ElectionComplete(nowNs int64)
}

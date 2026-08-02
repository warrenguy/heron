package consensus

// PositionTable is the agent-facing view of member log positions and quorum ranking.
// Implemented by the election host adapter; held by [Agent].
//
// UpdateMemberPosition takes memberID (no Member type). Election host keeps
// UpdateMemberLogPosition(*Member, ...) for election call sites.
//
// QuorumPositionBoundedByLeaderLog returns min(ranked quorum position,
// appendPosition).
type PositionTable interface {
	UpdateMemberPosition(memberID int32, leadershipTermID, logPosition int64)
	QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64

	// HasActiveQuorum reports whether a quorum of members reported an append
	// position within timeoutNs of nowNs. This member counts toward the
	// quorum. A settled leader keeps its own entry fresh every tick. A
	// single-member cluster always returns true, so a lone member never
	// reports a lost quorum.
	HasActiveQuorum(nowNs, timeoutNs int64) bool
}

// CanvassAnswer carries the leader-side values for a settled canvass answer.
//
// Behaviour:
//   - [Agent] fills every field from state it holds
//   - the egress resolves term base and successor-term fields from the
//     recording log
//   - the egress stamps the timestamp and this member's id, then publishes
//
// LogLeadershipTermID:
//   - is the canvasser's own value, echoed back unchanged
//   - a canvasser installs a leadership term only when the echo matches the
//     value it published
//   - an answer that substitutes the leader's own term is discarded by the
//     canvasser
type CanvassAnswer struct {
	// FollowerMemberID is the canvassing member. Member id 0 is valid.
	FollowerMemberID int32

	// LogLeadershipTermID is the canvasser's log leadership term, echoed back.
	LogLeadershipTermID int64

	// LeadershipTermID is this leader's current term.
	LeadershipTermID int64

	// LogPosition is this leader's cluster log publication position.
	LogPosition int64

	// CommitPosition is this leader's committed log position.
	CommitPosition int64

	// LeaderRecordingID is this leader's local log recording id.
	// Use the null recording id (-1) when no recording has started.
	LeaderRecordingID int64

	// LogSessionID is this leader's cluster log publication session id, or the
	// null session id (-1) when no publication is bound.
	//
	// [Agent] must derive this from the publication, not from
	// [LogPublisher.SessionID] alone:
	//   - that accessor returns 0 for an unbound publication
	//   - 0 is a valid session id
	LogSessionID int32
}

// ConsensusEgress is the agent-facing consensus-wire publish surface.
// Implemented by the election host adapter via the election publisher.
// [Agent] never holds the concrete publisher.
//
// AppendPosition publish target is [Agent.LeaderMemberID] (single store).
// HostAdapter.PublishAppendPosition reads the Agent field; egress holds no
// separate leader id.
type ConsensusEgress interface {
	PublishCommitPosition(commitPosition, leadershipTermID int64)
	PublishAppendPosition(leadershipTermID, logPosition int64, flags uint8) bool

	// PublishNewLeadershipTerm answers one member's canvass with this leader's
	// current leadership term.
	//
	// Behaviour:
	//   - resolves term base and successor-term fields from the recording log
	//   - publishes to answer.FollowerMemberID
	//
	// Returns false when:
	//   - the member is unknown
	//   - the member is this member
	//   - answer.LogSessionID is the null session id (-1)
	//   - the recording-log lookup is nil
	//   - this leader's current term has no recording-log entry
	//
	// Returns true when the publish was attempted.
	PublishNewLeadershipTerm(answer CanvassAnswer) bool
}

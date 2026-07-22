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
}

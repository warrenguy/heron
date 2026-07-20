package consensus

// PositionTable is the agent-facing view of member log positions and quorum ranking.
// Implemented by the election host adapter; held by [Agent].
//
// Method semantics:
//   - UpdateMemberPosition: memberID form (no Member type). The election host
//     keeps UpdateMemberLogPosition(*Member, ...) for election call sites.
//   - QuorumPositionBoundedByLeaderLog: min(ranked quorum position, appendPosition).
type PositionTable interface {
	UpdateMemberPosition(memberID int32, leadershipTermID, logPosition int64)
	QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64
}

// ConsensusEgress is the agent-facing consensus-wire publish surface.
// Implemented by the election host adapter via the election publisher.
// [Agent] never holds the concrete publisher.
//
// Method semantics:
//   - PublishCommitPosition: publish the cluster commit position
//   - PublishAppendPosition: publish a member append position with flags
type ConsensusEgress interface {
	PublishCommitPosition(commitPosition, leadershipTermID int64)
	PublishAppendPosition(leadershipTermID, logPosition int64, flags uint8) bool
}

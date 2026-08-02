package election

// CompareLog returns +1 if lhs is more up-to-date, 0 if equal, -1 if rhs wins.
// Tie-break: higher leadershipTermId wins; equal term -> higher logPosition wins.
func CompareLog(lhsTerm, lhsPos, rhsTerm, rhsPos int64) int {
	if lhsTerm > rhsTerm {
		return 1
	}
	if lhsTerm < rhsTerm {
		return -1
	}
	if lhsPos > rhsPos {
		return 1
	}
	if lhsPos < rhsPos {
		return -1
	}
	return 0
}

// CompareLogMembers compares election log state of lhs and rhs members.
func CompareLogMembers(lhs, rhs *Member) int {
	return CompareLog(
		lhs.LeadershipTermID, lhs.LogPosition,
		rhs.LeadershipTermID, rhs.LogPosition,
	)
}

// QuorumThreshold returns the minimum vote count for a quorum.
func QuorumThreshold(memberCount int) int {
	return (memberCount >> 1) + 1
}

// QuorumPosition returns the log position reached by a quorum of active members.
//
// Only members for which IsActive(nowNs, timeoutNs) is true contribute.
// LeadershipTermID is not consulted.
//
// ranked is a caller-owned scratch buffer. Its length is the ranking depth and
// must equal QuorumThreshold(len(members)) for majority semantics. The function
// zeros ranked on entry and returns ranked[len(ranked)-1]. No allocation is
// performed.
//
// An empty ranked panics.
//
// Concurrency: not safe for concurrent use of the same ranked buffer.
func QuorumPosition(members []*Member, ranked []int64, nowNs, timeoutNs int64) int64 {
	length := len(ranked)
	clear(ranked)

	for _, member := range members {
		if !member.IsActive(nowNs, timeoutNs) {
			continue
		}
		newPosition := member.LogPosition

		// Ranked stays descending; values at or below the floor never displace.
		if newPosition <= ranked[length-1] {
			continue
		}
		for i := range length {
			rankedPosition := ranked[i]
			if newPosition > rankedPosition {
				ranked[i] = newPosition
				newPosition = rankedPosition
			}
		}
	}

	return ranked[length-1]
}

// WillVoteFor reports whether member would vote for candidate.
// IsActive is deliberately omitted: during nomination, peers may stop advancing
// timeOfLastAppendPositionNs (crash, partition) and an active check would block candidacy.
func WillVoteFor(member, candidate *Member) bool {
	return member.LogPosition != NullPosition && CompareLogMembers(member, candidate) <= 0
}

// IsUnanimousLeader reports whether every non-graceful member voted true for candidateTermID.
func IsUnanimousLeader(
	members []*Member,
	candidateTermID int64,
	gracefulClosedLeaderID int32,
) bool {
	votes := 0
	for _, member := range members {
		if member.ID == gracefulClosedLeaderID {
			continue
		}
		if candidateTermID != member.CandidateTermID || !member.votedYes() {
			return false
		}
		votes++
	}
	return votes >= QuorumThreshold(len(members))
}

// IsQuorumLeader reports whether a quorum voted true with no explicit false votes.
func IsQuorumLeader(members []*Member, candidateTermID int64) bool {
	votes := 0
	for _, member := range members {
		if candidateTermID != member.CandidateTermID {
			continue
		}
		if member.votedNo() {
			return false
		}
		if member.votedYes() {
			votes++
		}
	}
	return votes >= QuorumThreshold(len(members))
}

// IsUnanimousCandidate reports whether every non-graceful member would vote for candidate.
func IsUnanimousCandidate(
	members []*Member,
	candidate *Member,
	gracefulClosedLeaderID int32,
) bool {
	possibleVotes := 0
	for _, member := range members {
		if member.ID == gracefulClosedLeaderID {
			continue
		}
		if !WillVoteFor(member, candidate) {
			return false
		}
		possibleVotes++
	}
	return possibleVotes >= QuorumThreshold(len(members))
}

// IsQuorumCandidate reports whether a quorum of members would vote for candidate.
func IsQuorumCandidate(members []*Member, candidate *Member) bool {
	possibleVotes := 0
	for _, member := range members {
		if WillVoteFor(member, candidate) {
			possibleVotes++
		}
	}
	return possibleVotes >= QuorumThreshold(len(members))
}

// HasQuorumAtPosition reports whether a quorum of active members reached position.
//
// Only members for which HasReachedPosition is true contribute. The threshold
// is QuorumThreshold(len(members)), so the denominator stays the full member
// count.
//
// Pair with HasActiveQuorum. That helper tests liveness alone. This helper also
// requires matching term and position.
func HasQuorumAtPosition(
	members []*Member,
	leadershipTermID, position, nowNs, timeoutNs int64,
) bool {
	votes := 0
	for _, member := range members {
		if member.HasReachedPosition(leadershipTermID, position, nowNs, timeoutNs) {
			votes++
		}
	}
	return votes >= QuorumThreshold(len(members))
}

// HasActiveQuorum reports whether a quorum of members reported an append
// position within timeoutNs of nowNs.
//
// Only members for which IsActive(nowNs, timeoutNs) is true contribute. The
// threshold is QuorumThreshold(len(members)), so the denominator stays the full
// member count.
//
// Pair with HasQuorumAtPosition. That helper also requires matching term and
// position. This helper tests liveness alone.
func HasActiveQuorum(members []*Member, nowNs, timeoutNs int64) bool {
	votes := 0
	for _, member := range members {
		if member.IsActive(nowNs, timeoutNs) {
			votes++
		}
	}
	return votes >= QuorumThreshold(len(members))
}

// BecomeCandidate sets ballot state for a new candidacy.
func BecomeCandidate(members []*Member, candidateTermID int64, candidateMemberID int32) {
	for _, member := range members {
		if member.ID == candidateMemberID {
			member.Vote = new(true)
			member.CandidateTermID = candidateTermID
			member.IsBallotSent = true
			continue
		}
		member.Vote = nil
		member.CandidateTermID = NullValue
		member.IsBallotSent = false
	}
}

// ResetMembers clears volatile election state on every member.
func ResetMembers(members []*Member) {
	for _, member := range members {
		member.Reset()
	}
}

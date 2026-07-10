package election

// Member represents a cluster member from the perspective of a single node.
type Member struct {
	ID           int32
	Vote         *bool
	IsBallotSent bool

	IsLeader                   bool
	HasTerminated              bool
	LeadershipTermID           int64
	CandidateTermID            int64
	LogPosition                int64
	TimeOfLastAppendPositionNs int64

	IngressEndpoint         string
	ConsensusEndpoint       string
	LogEndpoint             string
	CatchupEndpoint         string
	ArchiveEndpoint         string
	ArchiveResponseEndpoint string
	EgressResponseEndpoint  string
	Endpoints               string
}

// Reset clears volatile election state for a new election.
func (m *Member) Reset() {
	m.IsBallotSent = false
	m.IsLeader = false
	m.HasTerminated = false
	m.Vote = nil
	m.CandidateTermID = NullValue
	m.LeadershipTermID = NullValue
	m.LogPosition = NullPosition
}

// WillVoteFor reports whether this member would vote for candidate.
func (m *Member) WillVoteFor(candidate *Member) bool {
	return WillVoteFor(m, candidate)
}

func (m *Member) votedYes() bool {
	return m.Vote != nil && *m.Vote
}

func (m *Member) votedNo() bool {
	return m.Vote != nil && !*m.Vote
}

// IsActive reports whether append position was received within timeoutNs of nowNs.
func (m *Member) IsActive(nowNs, timeoutNs int64) bool {
	return m.TimeOfLastAppendPositionNs+timeoutNs > nowNs
}

// HasReachedPosition reports whether the member is active, on the expected term,
// and has persisted at least position.
func (m *Member) HasReachedPosition(
	leadershipTermID, position, nowNs, timeoutNs int64,
) bool {
	return m.IsActive(nowNs, timeoutNs) &&
		leadershipTermID == m.LeadershipTermID &&
		m.LogPosition >= position
}

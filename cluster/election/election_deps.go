package election

import "github.com/warrenguy/heron/cluster/consensus"

// StateObserver receives each applied election state transition. Production
// wiring updates the CLUSTER_ELECTION_STATE_TYPE_ID counter; tests record
// transitions.
type StateObserver interface {
	OnElectionStateChange(ElectionState)
}

type ElectionDeps struct {
	Handler   consensus.AgentHandler
	Host      ElectionHost
	Publisher Publisher

	NanoClock  func() int64
	EpochClock func() int64

	NodeState  CandidateTermStore
	Members    []*Member
	ThisMember *Member

	StateObserver StateObserver
}

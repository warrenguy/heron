//go:build testhooks

package sim

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/election"
)

// safetyObserver wraps the timeline StateObserver with safety checks on each
// election state change (via *ForTest hooks):
//   - candidate term must not decrease after the node has left Init
//   - CANDIDATE_BALLOT entry adds the self-vote to the vote ledger
//   - a ballot win must count only granted votes, at least a quorum, and no
//     FALSE vote
//   - first member to enter a self-leader state for a term is recorded in
//     leaderWins; a second member for the same term is a safety fault
type safetyObserver struct {
	StateObserver
	node *Node
	prev election.ElectionState
}

func newSafetyObserver(sim *ElectionSim, node *Node) election.StateObserver {
	return &safetyObserver{
		StateObserver: StateObserver{sim: sim, nodeID: node.id},
		node:          node,
	}
}

func init() {
	newElectionStateObserver = newSafetyObserver
}

func (o *safetyObserver) OnElectionStateChange(s election.ElectionState) {
	prev := o.prev
	o.prev = s
	o.StateObserver.OnElectionStateChange(s)

	n := o.node
	if n.election == nil {
		return
	}

	// Re-enter Init: stop tracking until the node leaves Init again and the
	// mark is re-seeded from the live candidate term.
	if s == election.ElectionStateInit {
		n.initLoaded = false
		return
	}
	if prev == election.ElectionStateInit && !n.initLoaded {
		n.initLoaded = true
		n.maxCandidateSeen = election.CandidateTermIDForTest(n.election)
	} else if n.initLoaded {
		if err := checkCandidateTermAdvance(n); err != nil {
			o.sim.recordSafetyErr(err)
		}
	}

	candidateTermID := election.CandidateTermIDForTest(n.election)
	if s == election.ElectionStateCandidateBallot {
		o.sim.recordSafetyErr(o.sim.votes.AddGrant(n.id, n.id, candidateTermID))
	}
	wonBallot := prev == election.ElectionStateCandidateBallot &&
		s == election.ElectionStateLeaderLogReplication
	if wonBallot {
		members := election.MembersForTest(n.election)
		o.sim.recordSafetyErr(o.sim.votes.CheckWin(n.id, candidateTermID, members))
		if !election.IsQuorumLeader(members, candidateTermID) {
			o.sim.recordSafetyErr(fmt.Errorf(
				"sim: member %d won the ballot at term %d without quorum",
				n.id, candidateTermID,
			))
		}
	}

	// First transition into a self-leader state for this term records the
	// winner; a second member for the same term is a fault.
	if IsSelfLeaderState(s) && !IsSelfLeaderState(prev) {
		term := n.election.LeadershipTermID()
		if existing, ok := o.sim.leaderWins[term]; ok && existing != n.id {
			o.sim.recordSafetyErr(fmt.Errorf(
				"sim: two self-leaders term=%d members=%d and %d",
				term, existing, n.id,
			))
		} else {
			o.sim.leaderWins[term] = n.id
		}
	}
}

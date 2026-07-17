//go:build testhooks

package sim

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/election"
)

// safetyObserver wraps the timeline StateObserver with two safety checks on
// each election state change (via *ForTest hooks):
//   - candidate term must not decrease after the node has left Init
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

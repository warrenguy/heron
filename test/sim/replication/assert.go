//go:build replicationsim && testhooks

package replication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// RequireSoftLiveness requires a live self-leader under replication mode.
//
// Soft liveness only: any live node in IsSelfLeaderState. Closed is not
// success (leaders stay LEADER_READY under replication host).
func RequireSoftLiveness(t *testing.T, s *sim.ElectionSim) {
	t.Helper()
	require.True(t, hasSoftLiveness(s), "soft liveness: expected live self-leader")
}

// hasSoftLiveness reports any node in IsSelfLeaderState.
func hasSoftLiveness(s *sim.ElectionSim) bool {
	for _, n := range s.Nodes() {
		el := n.Election()
		if el != nil && sim.IsSelfLeaderState(el.State()) {
			return true
		}
	}
	return false
}

// leaderReadyNode returns a node in LEADER_READY, or nil.
func leaderReadyNode(s *sim.ElectionSim) *sim.Node {
	for _, n := range s.Nodes() {
		el := n.Election()
		if el != nil && el.State() == election.ElectionStateLeaderReady {
			return n
		}
	}
	return nil
}

// runUntilSoftLiveness pumps until soft liveness holds or the step budget is exhausted.
func runUntilSoftLiveness(s *sim.ElectionSim, deadlineSteps int) error {
	return sim.PumpUntil(s, deadlineSteps, hasSoftLiveness)
}

// runUntilLeaderReady pumps until any live node is LEADER_READY.
func runUntilLeaderReady(s *sim.ElectionSim, deadlineSteps int) error {
	return sim.PumpUntil(s, deadlineSteps, func(s *sim.ElectionSim) bool {
		return leaderReadyNode(s) != nil
	})
}

// requireLeaderReady fails with scenario and phase when n is not LEADER_READY.
func requireLeaderReady(n *sim.Node, scenario, phase string) error {
	if st := n.Election().State(); st != election.ElectionStateLeaderReady {
		return fmt.Errorf("%s: %s want LEADER_READY got %v", scenario, phase, st)
	}
	return nil
}

// electLeaderReady pumps nodeID to LEADER_READY and returns its node and term.
func electLeaderReady(
	s *sim.ElectionSim,
	nodeID int32,
	deadlineSteps int,
) (*sim.Node, int64, error) {
	if err := sim.PumpUntilLeaderReady(s, nodeID, deadlineSteps); err != nil {
		return nil, 0, err
	}
	leader := s.Node(nodeID)
	return leader, leader.Election().LeadershipTermID(), nil
}

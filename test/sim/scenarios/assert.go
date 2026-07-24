//go:build electionsim && testhooks

package scenarios

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// RequireSoftLiveness requires live cluster leadership under a stable majority.
//
// Soft liveness only: does not require every member in LeaderReady.
// Satisfied when any live node is in a self-leader state, or any live node is
// Closed with a non-nil Leader(). Terminal Closed counts as success
// (completeElection finishes the ballot). The first-self-leader-per-term map
// (leaderWins) is not used: post-fault recovery must still show a live leader
// or Closed.
//
// Call only when ExpectLiveness is set.
func RequireSoftLiveness(t *testing.T, s *sim.ElectionSim) {
	t.Helper()
	require.True(t, hasSoftLiveness(s), "soft liveness: expected live cluster leadership")
}

func firstSelfLeaderNode(s *sim.ElectionSim) *sim.Node {
	for _, n := range s.Nodes() {
		el := n.Election()
		if el != nil && sim.IsSelfLeaderState(el.State()) {
			return n
		}
	}
	return nil
}

func hasSoftLiveness(s *sim.ElectionSim) bool {
	if firstSelfLeaderNode(s) != nil {
		return true
	}
	for _, n := range s.Nodes() {
		el := n.Election()

		// Terminal Closed after a completed election still has a Leader.
		if el != nil && el.State() == election.ElectionStateClosed && el.Leader() != nil {
			return true
		}
	}
	return false
}

// runUntilSoftLiveness pumps until soft liveness holds or the step budget is exhausted.
//
// deadlineSteps > 0 bounds progress from the call site using sim.Steps().
// Zero uses only Config.MaxSteps (ErrMaxSteps). Empty queues advance master
// time and re-tick live nodes.
func runUntilSoftLiveness(s *sim.ElectionSim, deadlineSteps int) error {
	return sim.PumpUntil(s, deadlineSteps, hasSoftLiveness)
}

// runDrainSteps processes up to n steps (or until queue idle pumps n times).
//
// Used while a partition holds, where liveness is not expected. Safety errors
// still surface. ErrQueueEmpty after pumps is not fatal until n is consumed.
func runDrainSteps(s *sim.ElectionSim, n int) error {
	for range n {
		err := s.RunSteps(1)
		if err == nil {
			continue
		}
		if errors.Is(err, sim.ErrQueueEmpty) {
			if err := sim.PumpIdle(s); err != nil {
				if errors.Is(err, sim.ErrQueueEmpty) {
					return nil
				}
				return err
			}
			continue
		}
		return err
	}
	return nil
}

// applyNemesis schedules one payload at Now and waits until it appears on the
// timeline. Soft liveness can already hold from a prior elect; waiting only
// on soft liveness would short-circuit and leave the nemesis unapplied.
func applyNemesis(s *sim.ElectionSim, payload sim.NemesisPayload, deadlineSteps int) error {
	return sim.ApplyNemesisBatch(s, payload.Action, 1, deadlineSteps, func() {
		s.ScheduleNemesis(s.Now(), payload)
	})
}

// leaderNode returns the live self-leader or Closed self-leader member, or nil.
func leaderNode(s *sim.ElectionSim) *sim.Node {
	if n := firstSelfLeaderNode(s); n != nil {
		return n
	}
	for _, n := range s.Nodes() {
		el := n.Election()
		if el == nil || el.State() != election.ElectionStateClosed {
			continue
		}
		if lead := el.Leader(); lead != nil && lead.ID == n.ID() {
			return n
		}
	}
	return nil
}

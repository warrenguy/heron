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
// or Closed. Call only when ExpectLiveness is set.
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

// pumpAdvanceNs advances idle master time by one election status interval.
//
// Matches ElectionStatusIntervalNs (100ms) so quiet gaps re-tick on the same
// cadence as production status polls instead of oversampling every 10ms.
const pumpAdvanceNs = int64(100_000_000)

// runUntilSoftLiveness pumps until soft liveness holds or the step budget is exhausted.
//
// deadlineSteps > 0 bounds progress from the call site using sim.Steps().
// Zero uses only Config.MaxSteps (ErrMaxSteps). Empty queues advance master
// time and re-tick live nodes.
func runUntilSoftLiveness(s *sim.ElectionSim, deadlineSteps int) error {
	return runUntil(s, deadlineSteps, hasSoftLiveness)
}

// runUntil steps until pred is true, deadlineSteps from the call site elapse,
// or Config.MaxSteps is hit.
//
// deadlineSteps <= 0 disables the local budget (sim MaxSteps still applies).
// Local budget is measured with ElectionSim.Steps so pumpIdle AdvanceToPhase
// steps count.
func runUntil(s *sim.ElectionSim, deadlineSteps int, pred func(*sim.ElectionSim) bool) error {
	start := s.Steps()
	for {
		if pred(s) {
			return nil
		}
		if deadlineSteps > 0 && s.Steps()-start >= deadlineSteps {
			return sim.ErrMaxSteps
		}

		err := s.RunSteps(1)
		if err == nil {
			continue
		}
		if errors.Is(err, sim.ErrQueueEmpty) {
			if err := pumpIdle(s); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

// pumpIdle advances time and schedules one tick per live node.
func pumpIdle(s *sim.ElectionSim) error {
	target := s.Now() + pumpAdvanceNs
	live := false
	for _, n := range s.Nodes() {
		if !s.IsLive(n.ID()) {
			continue
		}
		live = true
		if err := s.AdvanceToPhase(n.ID(), target); err != nil {
			return err
		}
	}
	if !live {
		return sim.ErrQueueEmpty
	}
	return nil
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
			if err := pumpIdle(s); err != nil {
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
	return applyNemesisBatch(s, payload.Action, 1, deadlineSteps, func() {
		s.ScheduleNemesis(s.Now(), payload)
	})
}

// applyNemesisBatch runs schedule then waits for count matching action rows.
func applyNemesisBatch(
	s *sim.ElectionSim,
	action string,
	count int,
	deadlineSteps int,
	schedule func(),
) error {
	mark := s.TimelineLen()
	schedule()
	return runUntilNemesisCount(s, mark, action, count, deadlineSteps)
}

// runUntilNemesisCount steps until at least count timeline entries with
// Kind "nemesis" and the given Action appear after mark (TimelineLen before
// ScheduleNemesis).
//
// Scans with a cursor so each step only clones the unprocessed tail.
func runUntilNemesisCount(
	s *sim.ElectionSim,
	mark int,
	action string,
	count int,
	deadlineSteps int,
) error {
	if count <= 0 {
		return nil
	}
	scanned := mark
	seen := 0
	return runUntil(s, deadlineSteps, func(s *sim.ElectionSim) bool {
		entries := s.TimelineSince(scanned)
		scanned += len(entries)
		for _, e := range entries {
			if e.Kind == "nemesis" && e.Action == action {
				seen++
				if seen >= count {
					return true
				}
			}
		}
		return false
	})
}

// schedulePartitionEdges schedules cut/heal for each edge.
func schedulePartitionEdges(
	s *sim.ElectionSim,
	atNs int64,
	action string,
	edges [][2]int32,
	bidirectional bool,
) {
	for _, e := range edges {
		s.ScheduleNemesis(atNs, sim.NemesisPayload{
			Action: action,
			Data: sim.PartitionSpec{
				From:          e[0],
				To:            e[1],
				Bidirectional: bidirectional,
			},
		})
	}
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

//go:build testhooks

package sim

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/election"
)

func init() {
	continuousSafetyAfterStep = checkContinuousSafety
}

// IsSelfLeaderState reports whether the election state means this member
// treats itself as leader (LeaderReady and related leader states).
//
// Host role is sticky after some error paths leave Init without RoleSet.
func IsSelfLeaderState(s election.ElectionState) bool {
	return s == election.ElectionStateLeaderLogReplication ||
		s == election.ElectionStateLeaderReplay ||
		s == election.ElectionStateLeaderInit ||
		s == election.ElectionStateLeaderReady
}

// CheckNoSplitBrainLive fails when two live elections share a leadership term
// and both are in a self-leader state.
//
// Pairwise over the small node list: runs after every step, so no per-call
// map allocation.
func CheckNoSplitBrainLive(s *ElectionSim) error {
	for i, a := range s.nodes {
		termA, ok := liveSelfLeaderTerm(a)
		if !ok {
			continue
		}
		for _, b := range s.nodes[i+1:] {
			termB, ok := liveSelfLeaderTerm(b)
			if !ok || termB != termA {
				continue
			}
			return fmt.Errorf(
				"sim: two live self-leaders term=%d members=%d and %d",
				termA, a.id, b.id,
			)
		}
	}
	return nil
}

// CheckElectionSafetyHistory compares each live self-leader to leaderWins for
// dump clarity. If the map already names a different winner for that term, it
// fails.
//
// Primary enforcement is the observer path that records the first member to
// enter a self-leader state for a term; this re-read is for dumps and asserts.
func CheckElectionSafetyHistory(s *ElectionSim) error {
	if s.safetyErr != nil {
		return s.safetyErr
	}

	// First member to win a term owns leaderWins[term]; conflicts already in safetyErr.
	for _, n := range s.nodes {
		term, ok := liveSelfLeaderTerm(n)
		if !ok {
			continue
		}
		if winner, ok := s.leaderWins[term]; ok && winner != n.id {
			return fmt.Errorf(
				"sim: live self-leader %d disagrees with recorded winner %d term=%d",
				n.id, winner, term,
			)
		}
	}
	return nil
}

func liveSelfLeaderTerm(n *Node) (int64, bool) {
	if n.election == nil || !IsSelfLeaderState(n.election.State()) {
		return 0, false
	}
	return n.election.LeadershipTermID(), true
}

// checkCandidateTermAdvance updates maxCandidateSeen; errors if the live
// candidate term went backward.
func checkCandidateTermAdvance(n *Node) error {
	ct := election.CandidateTermIDForTest(n.election)
	if ct < n.maxCandidateSeen {
		return fmt.Errorf(
			"sim: candidate term decreased member=%d seen=%d now=%d",
			n.id, n.maxCandidateSeen, ct,
		)
	}
	n.maxCandidateSeen = ct
	return nil
}

// CheckCandidateTermMonotonicity checks maxCandidateSeen on all live nodes
// that have left Init (initLoaded).
func CheckCandidateTermMonotonicity(s *ElectionSim) error {
	for _, n := range s.nodes {
		if n.election == nil || !n.initLoaded {
			continue
		}
		if err := checkCandidateTermAdvance(n); err != nil {
			return err
		}
	}
	return nil
}

// checkContinuousSafety runs after every step under testhooks.
//
// Always checks candidate-term monotonicity. Checks for two live self-leaders
// on the same term unless SkipSplitBrainCheck is set. When UseReplicationHost
// is set and SkipReplicationSafety is false, also runs continuous commit
// safety (monotonic, floor-on-advance, single authority).
func checkContinuousSafety(s *ElectionSim) error {
	if s.safetyErr != nil {
		return s.safetyErr
	}

	// Propose-max can advance without a state change; check every step.
	if err := CheckCandidateTermMonotonicity(s); err != nil {
		s.recordSafetyErr(err)
		return err
	}
	if !s.cfg.SkipSplitBrainCheck {
		if err := CheckNoSplitBrainLive(s); err != nil {
			s.recordSafetyErr(err)
			return err
		}
	}
	if s.cfg.UseReplicationHost && !s.cfg.SkipReplicationSafety {
		if err := checkReplicationCommitSafety(s); err != nil {
			s.recordSafetyErr(err)
			return err
		}
	}
	return nil
}

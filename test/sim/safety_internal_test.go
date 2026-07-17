//go:build testhooks

package sim

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

// First recordSafetyErr sticks; later calls do not overwrite. Timeline keeps
// one safety entry for the first violation only.
func TestSafety_recordFirstWins(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 50}, func(s *ElectionSim) {
		first := errors.New("first violation")
		second := errors.New("second violation")
		s.recordSafetyErr(first)
		s.recordSafetyErr(second)
		require.Equal(t, first, s.SafetyErr())

		found := false
		for _, e := range s.Timeline() {
			if e.Kind != "safety" {
				continue
			}
			found = true
			require.Contains(t, e.Detail, "first violation")
		}
		require.True(t, found)
	})
}

// RunSteps returns the recorded safety error after a step, not only via
// SafetyErr. Once a fault is stored, the next step surfaces it as the step error.
func TestSafety_stepSurfacesSafetyErr(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 51}, func(s *ElectionSim) {
		s.recordSafetyErr(errors.New("forced safety"))
		err := s.RunSteps(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "forced safety")
	})
}

// When a step is over MaxSteps and also has two live self-leaders, the error
// is the two-self-leaders fault, not ErrMaxSteps. Budget exhaustion must not
// hide safety failures that run after each step under testhooks.
func TestSafety_maxStepsDoesNotBurySafetyErr(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 1, MaxSteps: 10}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		forceTwoLiveSelfLeaders(s)
		// Spend the budget then take one more step so MaxSteps would fire
		// if safety were checked second or not at all.
		s.steps = s.cfg.MaxSteps
		err := s.RunSteps(1)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrMaxSteps)
		require.Contains(t, err.Error(), "two live self-leaders")
		require.Error(t, s.SafetyErr())
		require.Contains(t, s.SafetyErr().Error(), "two live self-leaders")
	})
}

// On transition into a self-leader state (Canvass -> LeaderReady here), the
// observer records the member as the first winner of that leadership term in
// leaderWins. A second member entering self-leader for the same term records
// "two self-leaders" into safetyErr and does not replace the first winner.
//
// SkipSplitBrainCheck only turns off the after-step scan of currently live
// dual self-leaders. The transition-time winner check still runs.
//
// Pre-seed leaderWins under node 1's current term so the forced transition on
// node 1 hits the conflict branch (terms can diverge after RunSteps).
func TestSafety_secondSelfLeaderForSameTerm(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 52, SkipSplitBrainCheck: true}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		n0 := s.Node(0)
		term := s.Node(1).election.LeadershipTermID()
		s.leaderWins[term] = n0.id

		forceSelfLeaderTransition(t, s.Node(1))

		err := s.SafetyErr()
		require.Error(t, err)
		require.Contains(t, err.Error(), "two self-leaders")
	})
}

// After each step (when armed), the sim fails if two live elections share a
// leadership term and both are in a self-leader state. Direct
// CheckNoSplitBrainLive and checkContinuousSafety both report that fault.
func TestSafety_liveSplitBrain(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 53}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		// Same default leadership term; force two live self-leaders.
		forceTwoLiveSelfLeaders(s)

		err := CheckNoSplitBrainLive(s)
		require.Error(t, err)
		require.Contains(t, err.Error(), "two live self-leaders")

		// Clear prior then re-run continuous so recording is attributed here.
		s.safetyErr = nil
		err = checkContinuousSafety(s)
		require.Error(t, err)
		safety := s.SafetyErr()
		require.Error(t, safety)
		require.Contains(t, safety.Error(), "two live self-leaders")
	})
}

// SkipSplitBrainCheck turns off only the after-step dual-self-leader scan.
// CheckNoSplitBrainLive still fails so a script can plant an illegal state and
// assert it without the step loop stopping on the after-step safety path.
func TestSafety_liveSplitBrainSkippedWhenDisarmed(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 54, SkipSplitBrainCheck: true}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		forceTwoLiveSelfLeaders(s)

		require.Error(t, CheckNoSplitBrainLive(s))
		require.NoError(t, checkContinuousSafety(s))
		require.NoError(t, s.SafetyErr())
	})
}

// Each node tracks the highest candidate term it has seen after leaving Init
// (maxCandidateSeen). Candidate terms must not go backward. Raising the mark
// above the live term trips both the direct check and the after-step path.
func TestSafety_candidateTermDecrease(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 55, SkipSplitBrainCheck: true}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		n := s.Node(0)
		require.True(t, n.initLoaded)

		// Artificially high mark: live candidate term looks like a decrease.
		n.maxCandidateSeen = election.CandidateTermIDForTest(n.election) + 100

		err := CheckCandidateTermMonotonicity(s)
		require.Error(t, err)
		require.Contains(t, err.Error(), "candidate term decreased")

		err = checkContinuousSafety(s)
		require.Error(t, err)
		require.Contains(t, err.Error(), "candidate term decreased")
		require.Contains(t, s.SafetyErr().Error(), "candidate term decreased")
	})
}

// Same candidate-term decrease via the state observer: after the node has left
// Init (initLoaded), a state-change callback re-checks maxCandidateSeen even
// when the state does not change. Prev and next are both Canvass, so no
// self-leader transition is recorded.
func TestSafety_candidateTermObserverDecrease(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 56, SkipSplitBrainCheck: true}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		n := s.Node(1)
		obs, ok := n.observer.(*safetyObserver)
		require.True(t, ok)

		n.maxCandidateSeen = election.CandidateTermIDForTest(n.election) + 50
		n.initLoaded = true
		obs.prev = election.ElectionStateCanvass
		obs.OnElectionStateChange(election.ElectionStateCanvass)

		err := s.SafetyErr()
		require.Error(t, err)
		require.Contains(t, err.Error(), "candidate term decreased")
	})
}

// CheckElectionSafetyHistory walks live self-leaders and compares each to
// leaderWins: if the map already names a different winner for that term, it
// fails. Used for dumps; primary conflict detection is the transition-time
// winner check. When safetyErr is already set, history returns that error
// without re-scanning.
func TestSafety_historyDisagreesWithLive(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 59, SkipSplitBrainCheck: true}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		n0 := s.Node(0)
		term := n0.election.LeadershipTermID()
		require.Equal(t, term, s.Node(1).election.LeadershipTermID())
		s.leaderWins[term] = n0.id

		// Node 1 is live self-leader; map says node 0 won the term.
		election.SetStateForTest(s.Node(1).election, election.ElectionStateLeaderReady)
		s.safetyErr = nil

		err := CheckElectionSafetyHistory(s)
		require.Error(t, err)
		require.Contains(t, err.Error(), "disagrees with recorded winner")

		// Early return when safetyErr already set; prior wins.
		prior := errors.New("prior safety")
		s.safetyErr = prior
		err = CheckElectionSafetyHistory(s)
		require.ErrorIs(t, err, prior)
	})
}

// LeaderWins is nil when empty and returns a defensive copy of the term ->
// first self-leader map. A conflicting second self-leader transition records
// safety but leaves the first winner for that term in the map.
//
// Pre-seed under node 1's term so the forced transition hits the conflict
// branch (same setup as TestSafety_secondSelfLeaderForSameTerm).
func TestLeaderWins_emptyAndCopy(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 70, SkipSplitBrainCheck: true}, func(s *ElectionSim) {
		require.Nil(t, s.LeaderWins())

		s.leaderWins[7] = 1
		got := s.LeaderWins()
		require.Equal(t, map[int64]int32{7: 1}, got)
		got[7] = 99
		require.Equal(t, int32(1), s.leaderWins[7], "LeaderWins must return a copy")

		require.NoError(t, s.RunSteps(1))
		n0 := s.Node(0)
		term := s.Node(1).election.LeadershipTermID()
		require.Equal(t, term, n0.election.LeadershipTermID())
		s.leaderWins[term] = n0.id

		// Conflict into safetyErr; first winner stays in the map.
		forceSelfLeaderTransition(t, s.Node(1))
		require.ErrorContains(t, s.SafetyErr(), "two self-leaders")
		wins := s.LeaderWins()
		require.Equal(t, n0.id, wins[term])
	})
}

// forceTwoLiveSelfLeaders pokes both elections into LeaderReady without firing
// the state observer. Use for after-step dual-self-leader checks that only
// care about current state, not transition-time winner recording.
func forceTwoLiveSelfLeaders(s *ElectionSim) {
	election.SetStateForTest(s.Node(0).election, election.ElectionStateLeaderReady)
	election.SetStateForTest(s.Node(1).election, election.ElectionStateLeaderReady)
}

// forceSelfLeaderTransition sets Canvass then notifies LeaderReady so the
// safety observer runs the first-self-leader-for-term path (leaderWins insert
// or conflict). Unlike forceTwoLiveSelfLeaders, this exercises transition logic.
func forceSelfLeaderTransition(t *testing.T, n *Node) {
	t.Helper()
	obs, ok := n.observer.(*safetyObserver)
	require.True(t, ok)
	obs.prev = election.ElectionStateCanvass
	election.SetStateForTest(n.election, election.ElectionStateLeaderReady)
	obs.OnElectionStateChange(election.ElectionStateLeaderReady)
}

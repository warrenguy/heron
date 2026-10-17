//go:build testhooks

package sim

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
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

// voteCheckConfig returns a focus sim config on node 1. Peers are inject
// sources.
func voteCheckConfig() Config {
	focus := int32(1)
	return Config{
		Seed:           42,
		MemberSpec:     DefaultThreeNodeMemberSpec(),
		ElectionConfig: DefaultElectionConfig(),
		MaxSteps:       500,
		FocusNodeID:    &focus,
	}
}

// ballotOnNodeOne drives node 1 to CANDIDATE_BALLOT at NullValue+1. Returns
// the first ballot deadline.
func ballotOnNodeOne(t *testing.T, s *ElectionSim) int64 {
	t.Helper()
	el := s.Node(1).Election()
	ecfg := s.cfg.ElectionConfig
	canvassDeadline := s.Now() + ecfg.StartupCanvassTimeoutNs

	require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
	require.NoError(t, InjectCanvassPosition(s, 0, 1, election.NullValue, 0))
	require.NoError(t, s.AdvanceToPhase(1, canvassDeadline+1))
	require.Equal(t, election.ElectionStateNominate, el.State())
	require.NoError(t, s.AdvanceToPhase(1, s.Now()+ecfg.ElectionTimeoutNs/2))
	require.Equal(t, election.ElectionStateCandidateBallot, el.State())
	require.Equal(t, election.NullValue+1, election.CandidateTermIDForTest(el))
	return s.Now() + ecfg.ElectionTimeoutNs
}

// placeGrant publishes a TRUE vote from voterID to candidateID at termID. The
// publish skips inject, so every send check runs.
func placeGrant(s *ElectionSim, voterID, candidateID, followerID int32, termID int64) {
	voter := s.Node(voterID)
	voter.Publisher().PlaceVote(
		election.PublicationTarget{MemberID: candidateID},
		codecs.VoteFields{
			CandidateTermID:     termID,
			LogLeadershipTermID: election.LogLeadershipTermIDForTest(voter.election),
			LogPosition:         voter.election.AppendPosition(),
			CandidateMemberID:   candidateID,
			FollowerMemberID:    followerID,
			Vote:                clusterwire.BooleanType_TRUE,
		},
	)
}

// TestSafety_voteChecks plants one vote fault per case and expects the error
// text of the check that the fault trips.
func TestSafety_voteChecks(t *testing.T) {
	t.Parallel()

	const termID = election.NullValue + 1
	const freshTermID = termID + 10
	tests := []struct {
		name  string
		plant func(t *testing.T, s *ElectionSim, ballotDeadline int64)
		want  string
	}{
		{
			name: "second grant",
			plant: func(_ *testing.T, s *ElectionSim, _ int64) {
				placeGrant(s, 1, 0, 1, termID)
			},
			want: "granted term",
		},
		{
			name: "wrong follower",
			plant: func(_ *testing.T, s *ElectionSim, _ int64) {
				placeGrant(s, 0, 1, 2, freshTermID)
			},
			want: "names follower",
		},
		{
			name: "no request",
			plant: func(_ *testing.T, s *ElectionSim, _ int64) {
				placeGrant(s, 0, 1, 0, freshTermID)
			},
			want: "answers no RequestVote",
		},
		{
			name: "ahead voter",
			plant: func(t *testing.T, s *ElectionSim, _ int64) {
				voter := s.Node(0).election
				position := voter.AppendPosition()
				scratch := &election.SbePublisher{}
				require.True(t, scratch.RequestVote(
					election.PublicationTarget{MemberID: 0},
					codecs.RequestVoteFields{
						LogLeadershipTermID: election.LogLeadershipTermIDForTest(voter),
						LogPosition:         position,
						CandidateTermID:     freshTermID,
						CandidateMemberID:   1,
					},
				))
				s.inbound = &DeliverPayload{From: 1, To: 0, Msg: scratch.Sent[0]}
				election.SetAppendPositionForTest(voter, position+1)
				placeGrant(s, 0, 1, 0, freshTermID)
			},
			want: "ahead of candidate",
		},
		{
			name: "stale vote",
			plant: func(t *testing.T, s *ElectionSim, ballotDeadline int64) {
				el := s.Node(1).Election()
				ecfg := s.cfg.ElectionConfig

				// Node 0's FALSE vote blocks the first ballot quorum.
				require.NoError(t, InjectVote(s, 2, 1, termID, election.NullValue, 0, true))
				require.NoError(t, InjectVote(s, 0, 1, termID, election.NullValue, 0, false))
				require.NoError(t, s.AdvanceToPhase(1, ballotDeadline+1))
				require.Equal(t, election.ElectionStateCanvass, el.State())

				require.NoError(t, InjectCanvassPosition(s, 0, 1, election.NullValue, 0))
				require.NoError(t, s.AdvanceToPhase(1, s.Now()+ecfg.LeaderHeartbeatTimeoutNs))
				require.Equal(t, election.ElectionStateNominate, el.State())
				require.NoError(t, s.AdvanceToPhase(1, s.Now()+ecfg.ElectionTimeoutNs))
				require.Equal(t, election.ElectionStateCandidateBallot, el.State())
				require.Equal(t, termID+1, election.CandidateTermIDForTest(el))
				require.NoError(t, s.SafetyErr())

				// Node 2 granted only at the first ballot term.
				member := election.MemberForTest(el, 2)
				member.Vote = new(true)
				member.CandidateTermID = termID + 1
				require.Error(t, s.AdvanceToPhase(1, s.Now()))
				require.Equal(t, election.ElectionStateCandidateBallot, el.State())
			},
			want: "without a grant",
		},
		{
			name: "short quorum",
			plant: func(t *testing.T, s *ElectionSim, _ int64) {
				n := s.Node(1)
				obs, ok := n.observer.(*safetyObserver)
				require.True(t, ok)
				obs.prev = election.ElectionStateCandidateBallot
				election.SetStateForTest(n.election, election.ElectionStateLeaderLogReplication)
				obs.OnElectionStateChange(election.ElectionStateLeaderLogReplication)
			},
			want: "short of quorum",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			Run(t, voteCheckConfig(), func(s *ElectionSim) {
				ballotDeadline := ballotOnNodeOne(t, s)
				require.NoError(t, s.SafetyErr())

				tt.plant(t, s, ballotDeadline)
				require.ErrorContains(t, s.SafetyErr(), tt.want)
			})
		})
	}
}

// After each step (when enabled), the sim fails if two live elections share a
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
// CheckNoSplitBrainLive still fails so a script can force an illegal state and
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

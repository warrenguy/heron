//go:build testhooks

package sim_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// IsSelfLeaderState is true for leader states where this member treats itself
// as leader; false for Canvass/Init and other non-leader states.
func TestIsSelfLeaderState(t *testing.T) {
	t.Parallel()

	for _, st := range []election.ElectionState{
		election.ElectionStateLeaderReady,
		election.ElectionStateLeaderInit,
		election.ElectionStateLeaderLogReplication,
		election.ElectionStateLeaderReplay,
	} {
		require.True(t, sim.IsSelfLeaderState(st), "%v", st)
	}
	for _, st := range []election.ElectionState{
		election.ElectionStateCanvass,
		election.ElectionStateInit,
	} {
		require.False(t, sim.IsSelfLeaderState(st), "%v", st)
	}
}

// Single-node elects; live two-self-leaders scan, history dump, and candidate
// term checks all stay clean.
func TestCheckNoSplitBrainLive_ok(t *testing.T) {
	t.Parallel()

	cfg := sim.Config{
		Seed:           41,
		MemberSpec:     sim.DefaultSingleNodeMemberSpec(),
		ElectionConfig: sim.DefaultElectionConfig(),
		MaxSteps:       500,
	}
	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		requireReaches(t, s, 0, election.ElectionStateLeaderReady)
		require.True(t, sim.IsSelfLeaderState(s.Node(0).Election().State()))
		require.NoError(t, sim.CheckNoSplitBrainLive(s))
		require.NoError(t, sim.CheckElectionSafetyHistory(s))
		require.NoError(t, sim.CheckCandidateTermMonotonicity(s))
		require.NoError(t, s.SafetyErr())
	})
}

// After all nodes leave Init, after-step safety stays quiet under a normal
// multi-node run (no dual self-leaders, no candidate-term drop).
func TestContinuousSafety_quietAfterLeaveInit(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 42}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(5))
		for _, n := range s.Nodes() {
			require.NotEqual(t, election.ElectionStateInit, n.Election().State())
		}
		require.NoError(t, s.SafetyErr())
		require.NoError(t, sim.CheckNoSplitBrainLive(s))
	})
}

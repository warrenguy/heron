//go:build electionsim && testhooks

package scenarios

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/sim"
)

// scenarioSplitVote randomizes nomination delay with light loss so ballots can
// split and recover. Soft liveness after settle.
func scenarioSplitVote() ScenarioSpec {
	return lossyScenario("split_vote", 200, sim.LossyPolicy{
		DropProb:   0.05,
		LatencyMin: 0,
		LatencyMax: 1_000_000,
	})
}

func TestScenario_splitVote(t *testing.T) {
	t.Parallel()
	RunScenario(t, scenarioSplitVote(), 1, 0)
}

func TestDeriveSeed_stableAndDistinct(t *testing.T) {
	t.Parallel()

	a := DeriveSeed(1, "split_vote", 0)
	b := DeriveSeed(1, "split_vote", 0)
	require.Equal(t, a, b)

	// Same base+index, different scenario names must differ.
	require.NotEqual(t, DeriveSeed(1, "split_vote", 0), DeriveSeed(1, "restart", 0))

	// Same base+scenario, different index must differ.
	require.NotEqual(t, DeriveSeed(1, "split_vote", 0), DeriveSeed(1, "split_vote", 1))

	// Different base must differ.
	require.NotEqual(t, DeriveSeed(1, "split_vote", 0), DeriveSeed(2, "split_vote", 0))

	// Catalog names stay collision-free across indices for the weight range.
	names := []string{
		"split_vote",
		"partition_majority",
		"partition_asymmetric",
		"restart",
		"clock_skew",
		"lossy_stable",
	}
	seen := make(map[uint64]struct{}, 1000)
	for _, name := range names {
		for i := range 250 {
			seed := DeriveSeed(1, name, i)
			_, ok := seen[seed]
			require.Falsef(t, ok, "seed collision seed=%d scenario=%s index=%d", seed, name, i)
			seen[seed] = struct{}{}
		}
	}
}

func TestNonZeroSeed_mapsZeroToAllOnes(t *testing.T) {
	t.Parallel()
	require.Equal(t, ^uint64(0), nonZeroSeed(0))
	require.Equal(t, uint64(1), nonZeroSeed(1))
	require.Equal(t, uint64(42), nonZeroSeed(42))
}

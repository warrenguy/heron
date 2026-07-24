//go:build replicationsim && testhooks

package replication

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/sim"
)

// TestReplicationSimSoak runs the weighted catalog serially.
//
// REPLICATION_SIM_RUNS scales scenario weights (default full 180).
// HERON_SIM_SEED overrides the base seed (shifts all derived seeds).
// -short skips the soak; TestScenarioSmoke still runs one fixed seed each.
func TestReplicationSimSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication sim soak under -short")
	}
	RunCatalog(t, sim.SoakConfig{
		BaseSeed:  1,
		Scenarios: replicationSimCatalog(),
	})
}

// TestScenarioSmoke runs every catalog scenario once at a fixed base seed.
func TestScenarioSmoke(t *testing.T) {
	t.Parallel()
	for _, sc := range replicationSimCatalog() {
		t.Run(sc.Name, func(t *testing.T) {
			t.Parallel()
			RunScenario(t, sc, 1, 0)
		})
	}
}

func TestCatalogWeightsSum(t *testing.T) {
	t.Parallel()
	require.Equal(t, DefaultCatalogWeight, sim.SumWeights(replicationSimCatalog()))
}

func TestScaleWeights(t *testing.T) {
	t.Parallel()

	// Trailing zero exercises zero-weight preservation under scaling.
	weights := []int{40, 50, 40, 30, 20, 20, 0}
	const total = 200

	tests := []struct {
		name   string
		target int
	}{
		{name: "zero target keeps weights", target: 0},
		{name: "target one", target: 1},
		{name: "target below scenario count", target: 5},
		{name: "target equals total keeps weights", target: total},
		{name: "target above total", target: 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := make([]sim.ScenarioSpec, len(weights))
			for i, w := range weights {
				in[i].Weight = w
			}

			out := sim.ScaleWeights(in, tc.target)
			require.Len(t, out, len(weights))

			if tc.target <= 0 || tc.target == total {
				for i, sc := range out {
					require.Equal(t, weights[i], sc.Weight)
				}
				return
			}

			sum := 0
			for i, sc := range out {
				sum += sc.Weight
				if weights[i] == 0 {
					require.Zero(t, sc.Weight)
					continue
				}

				// Largest remainder keeps each entry within one of exact share.
				floor := weights[i] * tc.target / total
				require.GreaterOrEqual(t, sc.Weight, floor)
				require.LessOrEqual(t, sc.Weight, floor+1)
			}
			require.Equal(t, tc.target, sum)
		})
	}
}

func TestDeriveSeed_stableAndDistinct(t *testing.T) {
	t.Parallel()

	a := sim.DeriveSeed(1, "single_node_commit", 0)
	b := sim.DeriveSeed(1, "single_node_commit", 0)
	require.Equal(t, a, b)

	// Same base+index, different scenario names must differ.
	require.NotEqual(t, a, sim.DeriveSeed(1, "follower_lag_commit", 0))

	// Same base+scenario, different index must differ.
	require.NotEqual(t, a, sim.DeriveSeed(1, "single_node_commit", 1))

	// Different base must differ.
	require.NotEqual(t, a, sim.DeriveSeed(2, "single_node_commit", 0))
}

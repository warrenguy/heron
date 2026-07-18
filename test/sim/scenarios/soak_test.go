//go:build electionsim && testhooks

package scenarios

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/sim"
)

// lossyScenario builds a soft-liveness scenario with a lossy network nemesis.
func lossyScenario(name string, weight int, policy sim.LossyPolicy) ScenarioSpec {
	return ScenarioSpec{
		Name:           name,
		Weight:         weight,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := baseConfig(seed)
			cfg.Nemesis = sim.NewLossyNetworkNemesis(policy)
			return cfg, func(s *sim.ElectionSim) error {
				return runUntilSoftLiveness(s, DefaultLivenessDeadlineSteps)
			}
		},
	}
}

// scenarioLossyStable keeps a majority fully connected under mild loss.
func scenarioLossyStable() ScenarioSpec {
	return lossyScenario("lossy_stable", 150, sim.LossyPolicy{
		DropProb:   0.1,
		LatencyMin: 0,
		LatencyMax: 2_000_000,
	})
}

// electionSimCatalog is the weighted scenario list (weights sum to 1000).
// restart_recordinglog is excluded.
func electionSimCatalog() []ScenarioSpec {
	return []ScenarioSpec{
		scenarioSplitVote(),
		scenarioPartitionMajority(),
		scenarioPartitionAsymmetric(),
		scenarioRestart(),
		scenarioClockSkew(),
		scenarioLossyStable(),
	}
}

// TestElectionSimSoak runs the weighted catalog serially.
//
// ELECTION_SIM_RUNS scales scenario weights (default full 1000).
// HERON_SIM_SEED overrides the base seed (shifts all derived seeds).
// -short skips the soak; standalone scenario tests still run one fixed seed.
func TestElectionSimSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping election sim soak under -short")
	}
	RunCatalog(t, SoakConfig{
		BaseSeed:  1,
		Scenarios: electionSimCatalog(),
	})
}

func TestCatalogWeightsSum1000(t *testing.T) {
	t.Parallel()
	sum := 0
	for _, sc := range electionSimCatalog() {
		sum += sc.Weight
	}
	if sum != DefaultCatalogWeight {
		t.Fatalf("catalog weight sum %d want %d", sum, DefaultCatalogWeight)
	}
}

func TestScaleWeights(t *testing.T) {
	t.Parallel()

	weights := []int{250, 250, 150, 150, 150, 50, 0}
	const total = 1000

	tests := []struct {
		name   string
		target int
	}{
		{name: "zero target keeps weights", target: 0},
		{name: "target one", target: 1},
		{name: "target below scenario count", target: 5},
		{name: "target equals total keeps weights", target: total},
		{name: "target above total", target: 2000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := make([]ScenarioSpec, len(weights))
			for i, w := range weights {
				in[i].Weight = w
			}

			out := scaleWeights(in, tc.target)
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

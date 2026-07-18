//go:build electionsim && testhooks

package scenarios

import (
	"testing"

	"github.com/warrenguy/heron/test/sim"
)

// majorityIsolateEdges cuts minority member 1 from majority {0,2}.
var majorityIsolateEdges = [][2]int32{{0, 1}, {2, 1}}

// scenarioPartitionMajority cuts 1 vs {0,2}, runs a bounded partitioned
// window, heals, then expects soft liveness. Liveness is not asserted while
// the partition holds.
func scenarioPartitionMajority() ScenarioSpec {
	const deadline = DefaultLivenessDeadlineSteps
	return ScenarioSpec{
		Name:           "partition_majority",
		Weight:         250,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			part := sim.NewPartitionNemesis()
			cfg := baseConfig(seed)
			cfg.Nemesis = part
			return cfg, func(s *sim.ElectionSim) error {
				if err := applyNemesisBatch(
					s, sim.ActionPartition, len(majorityIsolateEdges), deadline,
					func() {
						schedulePartitionEdges(
							s, s.Now(), sim.ActionPartition, majorityIsolateEdges, true,
						)
					},
				); err != nil {
					return err
				}

				// Bounded steps under the cut: no open-ended wait for a leader.
				if err := runDrainSteps(s, 800); err != nil {
					return err
				}

				// Apply heal before soft liveness: majority may already hold leadership.
				if err := applyNemesisBatch(
					s, sim.ActionHeal, len(majorityIsolateEdges), deadline,
					func() {
						schedulePartitionEdges(
							s, s.Now(), sim.ActionHeal, majorityIsolateEdges, true,
						)
					},
				); err != nil {
					return err
				}
				return runUntilSoftLiveness(s, deadline)
			}
		},
	}
}

// scenarioPartitionAsymmetric applies directed cuts only. Safety continuous;
// no soft liveness.
func scenarioPartitionAsymmetric() ScenarioSpec {
	return ScenarioSpec{
		Name:           "partition_asymmetric",
		Weight:         50,
		ExpectLiveness: false,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			part := sim.NewPartitionNemesis()
			cfg := baseConfig(seed)
			cfg.Nemesis = part
			return cfg, func(s *sim.ElectionSim) error {
				edges := [][2]int32{{0, 1}, {1, 2}}
				if err := applyNemesisBatch(
					s, sim.ActionPartition, len(edges), DefaultLivenessDeadlineSteps,
					func() {
						schedulePartitionEdges(s, s.Now(), sim.ActionPartition, edges, false)
					},
				); err != nil {
					return err
				}

				// Exercise the cut graph under continuous safety only.
				return runDrainSteps(s, 1200)
			}
		},
	}
}

func TestScenario_partitionMajority(t *testing.T) {
	t.Parallel()
	RunScenario(t, scenarioPartitionMajority(), 1, 0)
}

func TestScenario_partitionAsymmetric(t *testing.T) {
	t.Parallel()
	RunScenario(t, scenarioPartitionAsymmetric(), 1, 0)
}

//go:build electionsim && testhooks

package scenarios

import (
	"testing"

	"github.com/warrenguy/heron/test/sim"
)

// scenarioClockSkew advances epoch clocks mid-run then expects soft liveness.
func scenarioClockSkew() sim.ScenarioSpec {
	const deadline = sim.DefaultLivenessDeadlineSteps
	return sim.ScenarioSpec{
		Name:           "clock_skew",
		Weight:         150,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := baseConfig(seed)
			cfg.Nemesis = sim.ClockSkewNemesis{}
			return cfg, func(s *sim.ElectionSim) error {
				if err := runDrainSteps(s, 100); err != nil {
					return err
				}

				// Apply skew before soft liveness: elect may already have completed.
				if err := applyNemesis(s, sim.NemesisPayload{
					Action: sim.ActionSkew,
					Data: sim.SkewSpec{
						DeltaMs: 5_000,
						NodeIDs: []int32{0, 2},
					},
				}, deadline); err != nil {
					return err
				}
				return runUntilSoftLiveness(s, deadline)
			}
		},
	}
}

func TestScenario_clockSkew(t *testing.T) {
	t.Parallel()
	RunScenario(t, scenarioClockSkew(), 1, 0)
}

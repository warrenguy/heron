//go:build electionsim && testhooks

// Package scenarios holds the election fault scenario catalog and soak runner.
//
// Build with -tags=electionsim,testhooks. Not part of default unit packages.
package scenarios

import (
	"testing"

	"github.com/warrenguy/heron/test/sim"
)

// DefaultCatalogWeight is the sum of soak scenario weights.
const DefaultCatalogWeight = 1000

// soakRunner binds election catalog policy to the shared soak machinery.
var soakRunner = sim.SoakRunner{
	RunsEnv:         "ELECTION_SIM_RUNS",
	LogPrefix:       "scenarios",
	RequireLiveness: RequireSoftLiveness,
}

// RunCatalog executes every weighted seed serially (no t.Parallel).
//
// ErrMaxSteps and safety errors hard-fail the seed with DumpOnFail via sim.Run.
func RunCatalog(t *testing.T, cfg sim.SoakConfig) {
	t.Helper()
	soakRunner.RunCatalog(t, cfg)
}

// RunScenario executes one named scenario at a fixed base and run index.
func RunScenario(t *testing.T, sc sim.ScenarioSpec, base uint64, runIndex int) {
	t.Helper()
	soakRunner.RunScenario(t, sc, base, runIndex)
}

// baseConfig returns a three-node sim config with NominationRand from seed.
func baseConfig(seed uint64) sim.Config {
	cfg := sim.Config{
		Seed:           seed,
		MemberSpec:     sim.DefaultThreeNodeMemberSpec(),
		ElectionConfig: sim.DefaultElectionConfig(),
		MaxSteps:       sim.DefaultMaxSteps,
	}
	return sim.ConfigWithSimRand(cfg)
}

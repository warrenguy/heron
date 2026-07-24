//go:build replicationsim && testhooks

// Package replication holds the replication safety scenario catalog and soak runner.
//
// Build with -tags=replicationsim,testhooks. Not part of default unit packages.
package replication

import (
	"testing"

	"github.com/warrenguy/heron/test/sim"
)

// DefaultCatalogWeight is the sum of soak scenario weights.
const DefaultCatalogWeight = 180

// soakRunner binds replication catalog policy to the shared soak machinery.
//
// End-of-seed commit checks only; the continuous path already checks each step.
var soakRunner = sim.SoakRunner{
	RunsEnv:         "REPLICATION_SIM_RUNS",
	LogPrefix:       "replication",
	RequireLiveness: RequireSoftLiveness,
	PostSeedChecks: []func(*sim.ElectionSim) error{
		sim.CheckCommitMonotonic,
		sim.CheckCommitBoundedByModelQuorum,
		sim.CheckSingleCommitAuthority,
	},
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

// baseConfig returns a three-node replication-host sim config.
func baseConfig(seed uint64) sim.Config {
	cfg := sim.Config{
		Seed:               seed,
		MemberSpec:         sim.DefaultThreeNodeMemberSpec(),
		ElectionConfig:     sim.DefaultElectionConfig(),
		MaxSteps:           sim.DefaultMaxSteps,
		UseReplicationHost: true,
	}
	return sim.ConfigWithSimRand(cfg)
}

// singleNodeConfig returns a one-node replication-host sim config.
func singleNodeConfig(seed uint64) sim.Config {
	cfg := baseConfig(seed)
	cfg.MemberSpec = sim.DefaultSingleNodeMemberSpec()
	return cfg
}

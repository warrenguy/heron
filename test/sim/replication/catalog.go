//go:build replicationsim && testhooks

package replication

import "github.com/warrenguy/heron/test/sim"

// replicationSimCatalog is the weighted scenario list (weights sum to
// DefaultCatalogWeight).
func replicationSimCatalog() []sim.ScenarioSpec {
	return []sim.ScenarioSpec{
		scenarioSingleNodeCommit(),
		scenarioFollowerLagCommit(),
		scenarioPartitionDuringCommit(),
		scenarioCommitAuthority(),
		scenarioAppendInjectRoundtrip(),
	}
}

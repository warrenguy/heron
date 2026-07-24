//go:build replicationsim && testhooks

package replication

import (
	"fmt"

	"github.com/warrenguy/heron/test/sim"
)

// scenarioSingleNodeCommit elects N=1, raises append, asserts Agent commit.
func scenarioSingleNodeCommit() sim.ScenarioSpec {
	const (
		deadline           = sim.DefaultLivenessDeadlineSteps
		leaderAppend int64 = 1024
	)
	return sim.ScenarioSpec{
		Name:           "single_node_commit",
		Weight:         40,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := singleNodeConfig(seed)
			return cfg, func(s *sim.ElectionSim) error {
				// Natural elect to LEADER_READY.
				if err := runUntilLeaderReady(s, deadline); err != nil {
					return err
				}
				leader := leaderReadyNode(s)
				if leader == nil {
					return fmt.Errorf("single_node_commit: no LEADER_READY after elect")
				}

				// Raise append; still READY not Closed.
				sim.SetLeaderAppend(leader, leaderAppend)
				if leader.Election().AppendPosition() != leaderAppend {
					return fmt.Errorf(
						"single_node_commit: append want %d got %d",
						leaderAppend, leader.Election().AppendPosition(),
					)
				}
				if err := requireLeaderReady(leader, "single_node_commit", "after raise"); err != nil {
					return err
				}

				// Tick >= 3 x LeaderHeartbeatIntervalNs; Agent commit required.
				interval := cfg.ElectionConfig.LeaderHeartbeatIntervalNs
				if err := sim.PumpNodes(s, interval, 4); err != nil {
					return err
				}
				if err := requireLeaderReady(leader, "single_node_commit", "after ticks"); err != nil {
					return err
				}
				agent := sim.AgentCommitPosition(leader)
				if agent != leaderAppend {
					return fmt.Errorf(
						"single_node_commit: Agent.CommitPosition want %d got %d",
						leaderAppend, agent,
					)
				}

				// Wire CommitPosition empty is OK for N=1 (HostAdapter skips self).
				return nil
			}
		},
	}
}

//go:build replicationsim && testhooks

package replication

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// scenarioAppendInjectRoundtrip is the 3-node elect + raise + inject smoke path.
func scenarioAppendInjectRoundtrip() sim.ScenarioSpec {
	const (
		deadline           = sim.DefaultLivenessDeadlineSteps
		leaderAppend int64 = 10_000
	)
	return sim.ScenarioSpec{
		Name:           "append_inject_roundtrip",
		Weight:         20,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := baseConfig(seed)
			cfg.ElectionConfig.AppointedLeaderID = 0
			return cfg, func(s *sim.ElectionSim) error {
				leader, term, err := electLeaderReady(s, 0, deadline)
				if err != nil {
					return err
				}
				interval := cfg.ElectionConfig.LeaderHeartbeatIntervalNs

				sim.SetLeaderAppend(leader, leaderAppend)
				if err := sim.PumpNodes(s, interval, 5); err != nil {
					return err
				}

				// Followers must not hang in CatchupInit under raised append.
				for _, n := range s.Nodes() {
					el := n.Election()
					if el != nil && el.State() == election.ElectionStateFollowerCatchupInit {
						return fmt.Errorf(
							"append_inject_roundtrip: member %d stuck in CatchupInit",
							n.ID(),
						)
					}
				}

				for _, fid := range []int32{1, 2} {
					if err := sim.InjectAppendPosition(s, fid, 0, term, leaderAppend); err != nil {
						return err
					}
				}
				if err := sim.PumpNodes(s, interval, 10); err != nil {
					return err
				}

				// Wire high-water at raised append proves inject + rank path.
				if sim.WireCommitHighWater(leader, term) != leaderAppend {
					return fmt.Errorf(
						"append_inject_roundtrip: expected CommitPosition logPosition=%d term=%d",
						leaderAppend, term,
					)
				}
				return requireLeaderReady(leader, "append_inject_roundtrip", "after inject")
			}
		},
	}
}

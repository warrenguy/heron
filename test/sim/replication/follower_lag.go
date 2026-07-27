//go:build replicationsim && testhooks

package replication

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// scenarioFollowerLagCommit elects, raises append, stepwise injects followers.
func scenarioFollowerLagCommit() sim.ScenarioSpec {
	const (
		deadline           = sim.DefaultLivenessDeadlineSteps
		leaderAppend int64 = 10_000
		halfAppend   int64 = leaderAppend / 2
	)
	return sim.ScenarioSpec{
		Name:           "follower_lag_commit",
		Weight:         50,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := baseConfig(seed)

			// Elect first at low append; raise only after LEADER_READY.
			cfg.ElectionConfig.AppointedLeaderID = 0
			return cfg, func(s *sim.ElectionSim) error {
				leader, term, err := electLeaderReady(s, 0, deadline)
				if err != nil {
					return err
				}
				interval := cfg.ElectionConfig.LeaderHeartbeatIntervalNs

				// Raise leader append after READY.
				sim.SetLeaderAppend(leader, leaderAppend)
				if err := requireLeaderReady(leader, "follower_lag_commit", "after raise"); err != nil {
					return err
				}

				// Inject follower 1 at L/2; deliver; tick.
				if err := sim.InjectAppendPosition(s, 1, 0, term, halfAppend); err != nil {
					return err
				}
				if err := sim.PumpNodes(s, interval, 3); err != nil {
					return err
				}
				f1 := election.MemberForTest(leader.Election(), 1)
				if f1 == nil {
					return fmt.Errorf("follower_lag_commit: member 1 missing on leader table")
				}
				if f1.LogPosition != halfAppend {
					return fmt.Errorf(
						"follower_lag_commit: f1 LogPosition want %d got %d",
						halfAppend, f1.LogPosition,
					)
				}

				// Continuous floor-on-advance; commit may lag L.
				sig := sim.CommitSignalHighWater(s, leader)
				floor := sim.ModelQuorumFloor(leader, s.Now())
				if sig > floor {
					return fmt.Errorf(
						"follower_lag_commit: half-inject signal %d above floor %d",
						sig, floor,
					)
				}
				if err := requireLeaderReady(leader, "follower_lag_commit", "after half-inject"); err != nil {
					return err
				}

				// Raise both followers to L so quorum floor can hit L (threshold 2 of 3).
				for _, fid := range []int32{2, 1} {
					if err := sim.InjectAppendPosition(s, fid, 0, term, leaderAppend); err != nil {
						return err
					}
				}
				if err := sim.PumpNodes(s, interval, 10); err != nil {
					return err
				}
				sig = sim.CommitSignalHighWater(s, leader)
				if sig != leaderAppend {
					return fmt.Errorf(
						"follower_lag_commit: full-inject commit want %d got %d (wire=%d agent=%d)",
						leaderAppend, sig,
						sim.WireCommitHighWater(leader, term),
						sim.AgentCommitPosition(leader),
					)
				}
				return requireLeaderReady(leader, "follower_lag_commit", "after full-inject")
			}
		},
	}
}

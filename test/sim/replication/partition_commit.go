//go:build replicationsim && testhooks

package replication

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// majorityIsolateEdges cuts minority member 1 from majority {0,2}.
var majorityIsolateEdges = [][2]int32{{0, 1}, {2, 1}}

// scenarioPartitionDuringCommit isolates minority, ages stamps carefully,
// refreshes majority, then raises commit.
func scenarioPartitionDuringCommit() sim.ScenarioSpec {
	const (
		name               = "partition_during_commit"
		deadline           = sim.DefaultLivenessDeadlineSteps
		leaderAppend int64 = 8_000
	)
	return sim.ScenarioSpec{
		Name:           name,
		Weight:         40,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := baseConfig(seed)
			cfg.Nemesis = sim.NewPartitionNemesis()

			// Prefer leader in majority {0,2}.
			cfg.ElectionConfig.AppointedLeaderID = 0
			return cfg, func(s *sim.ElectionSim) error {
				// Elect; leader in majority.
				leader, term, err := electLeaderReady(s, 0, deadline)
				if err != nil {
					return err
				}
				interval := cfg.ElectionConfig.LeaderHeartbeatIntervalNs
				timeoutNs := cfg.ElectionConfig.LeaderHeartbeatTimeoutNs

				applyEdges := func(action string) error {
					return sim.ApplyNemesisBatch(
						s, action, len(majorityIsolateEdges), deadline,
						func() {
							sim.SchedulePartitionEdges(
								s, s.Now(), action, majorityIsolateEdges, true,
							)
						},
					)
				}
				injectMajority := func() error {
					return sim.InjectAppendPosition(s, 2, 0, term, leaderAppend)
				}

				// Isolate minority member 1.
				if err := applyEdges(sim.ActionPartition); err != nil {
					return err
				}

				// Raise append while partitioned.
				sim.SetLeaderAppend(leader, leaderAppend)

				// Age peer stamps past the activity window without minority re-inject.
				// AdvanceToPhase may re-stamp leader thisMember on tick; peer stamps go
				// inactive by wall advance. Only minority must stay stale after majority
				// re-inject below.
				if _, err := sim.AdvanceLive(s, s.Now()+timeoutNs+1); err != nil {
					return err
				}

				// Refresh majority peer only (member 2 -> leader 0).
				// Minority stays stale on leader table.
				if err := injectMajority(); err != nil {
					return err
				}

				// Tick leader so thisMember stamps and commit path runs.
				if err := sim.PumpNodes(s, interval, 4); err != nil {
					return err
				}

				// Minority stale; majority peer active; quorum still reachable.
				nowNs := s.Now()
				members := election.MembersForTest(leader.Election())
				var minority, majorityPeer *election.Member
				active := 0
				for _, m := range members {
					switch m.ID {
					case 1:
						minority = m
					case 2:
						majorityPeer = m
					}
					if m.IsActive(nowNs, timeoutNs) {
						active++
					}
				}
				if minority == nil || majorityPeer == nil {
					return fmt.Errorf("%s: missing members on leader table", name)
				}
				if minority.IsActive(nowNs, timeoutNs) {
					return fmt.Errorf(
						"%s: minority still active stamp=%d now=%d",
						name, minority.TimeOfLastAppendPositionNs, nowNs,
					)
				}
				if !majorityPeer.IsActive(nowNs, timeoutNs) {
					return fmt.Errorf(
						"%s: majority peer inactive stamp=%d now=%d",
						name, majorityPeer.TimeOfLastAppendPositionNs, nowNs,
					)
				}
				if threshold := election.QuorumThreshold(len(members)); active < threshold {
					return fmt.Errorf(
						"%s: active %d < threshold %d",
						name, active, threshold,
					)
				}

				// Raise commit via ticks; floor check only when signal advances.
				sig := sim.CommitSignalHighWater(s, leader)
				if sig < leaderAppend {
					// Re-inject majority and pump until commit catches up.
					if err := injectMajority(); err != nil {
						return err
					}
					if err := sim.PumpNodes(s, interval, 10); err != nil {
						return err
					}
					sig = sim.CommitSignalHighWater(s, leader)
				}
				if sig != leaderAppend {
					return fmt.Errorf(
						"%s: commit want %d got %d",
						name, leaderAppend, sig,
					)
				}
				floor := sim.ModelQuorumFloor(leader, s.Now())
				if sig > floor {
					return fmt.Errorf(
						"%s: signal %d above floor %d",
						name, sig, floor,
					)
				}

				// Heal; soft liveness (leader IsSelfLeaderState).
				if err := applyEdges(sim.ActionHeal); err != nil {
					return err
				}
				return runUntilSoftLiveness(s, deadline)
			}
		},
	}
}

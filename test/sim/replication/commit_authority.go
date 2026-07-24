//go:build replicationsim && testhooks

package replication

import (
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/test/sim"
)

// scenarioCommitAuthority checks CommitPosition Sent only from leader member id.
func scenarioCommitAuthority() sim.ScenarioSpec {
	const (
		deadline           = sim.DefaultLivenessDeadlineSteps
		leaderAppend int64 = 10_000
	)
	return sim.ScenarioSpec{
		Name:           "commit_authority",
		Weight:         30,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := baseConfig(seed)
			cfg.ElectionConfig.AppointedLeaderID = 0
			return cfg, func(s *sim.ElectionSim) error {
				// Elect + SetLeaderAppend + inject enough for publish.
				leader, term, err := electLeaderReady(s, 0, deadline)
				if err != nil {
					return err
				}
				interval := cfg.ElectionConfig.LeaderHeartbeatIntervalNs

				sim.SetLeaderAppend(leader, leaderAppend)
				for _, fid := range []int32{1, 2} {
					if err := sim.InjectAppendPosition(s, fid, 0, term, leaderAppend); err != nil {
						return err
					}
				}
				if err := sim.PumpNodes(s, interval, 10); err != nil {
					return err
				}

				if err := sim.CheckSingleCommitAuthority(s); err != nil {
					return err
				}
				return commitAuthorityOnWire(s, term, leader.ID())
			}
		},
	}
}

// commitAuthorityOnWire requires at least one CommitPosition for term on any
// node publisher and LeaderMemberID == wantLeaderID on every such message.
func commitAuthorityOnWire(s *sim.ElectionSim, term int64, wantLeaderID int32) error {
	seen := false
	for _, n := range s.Nodes() {
		pub := n.Publisher()
		if pub == nil {
			continue
		}
		for _, msg := range pub.Sent {
			if msg.TemplateID != clusterwire.CommitPositionSbeTemplateID {
				continue
			}
			fields, ok := msg.Fields.(codecs.CommitPositionFields)
			if !ok || fields.LeadershipTermID != term {
				continue
			}
			seen = true
			if fields.LeaderMemberID != wantLeaderID {
				return fmt.Errorf(
					"commit_authority: CommitPosition LeaderMemberID=%d want %d",
					fields.LeaderMemberID, wantLeaderID,
				)
			}
		}
	}
	if !seen {
		return fmt.Errorf(
			"commit_authority: no CommitPosition on wire for term %d",
			term,
		)
	}
	return nil
}

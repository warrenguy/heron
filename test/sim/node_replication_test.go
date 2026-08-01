//go:build testhooks

package sim_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// Default host type is simElectionHost; election catalog path unchanged.
func TestNode_defaultHostIsSimElectionHost(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 501}, func(s *sim.ElectionSim) {
		for _, n := range s.Nodes() {
			require.True(t, sim.HostIsSimElectionHost(n))
			require.Nil(t, n.AgentCM())
			n.SetHostRole(consensus.RoleCandidate)
			require.Equal(t, consensus.RoleCandidate, n.HostRole())
		}
	})
}

// Replication mode SetHostRole / HostRole round-trips through Agent.
func TestNode_replicationHost_roleViaAgent(t *testing.T) {
	t.Parallel()

	cfg := sim.Config{
		Seed:               502,
		UseReplicationHost: true,
		ElectionConfig:     sim.DefaultElectionConfig(),
	}
	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		n := s.Node(0)
		agent := n.AgentCM()
		require.NotNil(t, agent)
		require.True(t, sim.HostIsSimReplicationHost(n))

		require.Equal(t, consensus.RoleFollower, n.HostRole())
		n.SetHostRole(consensus.RoleLeader)
		require.Equal(t, consensus.RoleLeader, n.HostRole())
		require.Equal(t, consensus.RoleLeader, agent.Role())

		n.SetHostRole(consensus.RoleCandidate)
		require.Equal(t, consensus.RoleCandidate, n.HostRole())
		require.Equal(t, consensus.RoleCandidate, agent.Role())
	})
}

// Sim host overrides: session id, refuse close, near-live catchup, join at start.
func TestNode_replicationHost_simOverrides(t *testing.T) {
	t.Parallel()

	cfg := sim.Config{
		Seed:               505,
		UseReplicationHost: true,
		ElectionConfig:     sim.DefaultElectionConfig(),
	}
	// Matches package sim simLogSessionID (unexported; sim_test cannot read it).
	const logSessionID int32 = 777

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		h := s.Node(0).Host()
		require.NotNil(t, h)

		// Session id for NLT control wire.
		require.Equal(t, logSessionID, h.AddLogPublication(0))
		require.Equal(t, logSessionID, h.AddLogPublication(99))

		// Refuse election close; leader stays LEADER_READY under raised append.
		require.False(t, h.AppendNewLeadershipTermEvent(0))
		require.False(t, h.AppendNewLeadershipTermEvent(1_000_000_000))

		// Near-live so CatchupInit does not stall without archive.
		require.True(t, h.IsCatchupNearLive(0))
		require.True(t, h.IsCatchupNearLive(10_000))

		// Follower join position matches scripted election host.
		params, ok := h.FollowerLogJoinReady(logSessionID)
		require.True(t, ok)
		require.Equal(t, consensus.FollowerLogJoinParams{
			LogChannel:      "log=sim",
			LogStreamID:     100,
			LogSessionID:    logSessionID,
			LogJoinPosition: 0,
		}, params)
	})
}

// Restart under UseReplicationHost rebuilds HostAdapter path.
func TestNode_replicationHost_restartKeepsAdapter(t *testing.T) {
	t.Parallel()

	cfg := sim.Config{
		Seed:               503,
		UseReplicationHost: true,
		ElectionConfig:     sim.DefaultElectionConfig(),
		MaxSteps:           500,
	}
	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(3))
		n := s.Node(1)
		require.True(t, sim.HostIsSimReplicationHost(n))
		agentBefore := n.AgentCM()
		require.NotNil(t, agentBefore)

		require.NoError(t, s.CrashNode(1))
		crashed := s.Node(1)
		require.Nil(t, crashed.Election())
		require.Nil(t, crashed.Host())
		require.Nil(t, crashed.AgentCM())

		require.NoError(t, s.RestartNode(1))
		restored := s.Node(1)
		require.NotNil(t, restored.Election())
		require.True(t, sim.HostIsSimReplicationHost(restored))
		agent := restored.AgentCM()
		require.NotNil(t, agent)

		// Fresh Agent on reattach (not the crashed instance).
		require.NotSame(t, agentBefore, agent)

		// Append source tracks election high-water after leave Init.
		src := agent.AppendSource()
		require.NotNil(t, src)
		got, ready := src.AppendPosition()
		require.True(t, ready)
		require.Equal(t, restored.Election().AppendPosition(), got)
	})
}

// Acceptance smoke: 3-node elect, raise leader append, inject follower
// progress, observe CommitPosition at that append on the wire; leader stays
// LEADER_READY; followers do not hang in CatchupInit.
func TestReplicationHost_acceptanceSmoke(t *testing.T) {
	t.Parallel()

	const leaderAppend int64 = 10_000

	cfg := sim.Config{
		Seed:               504,
		MemberSpec:         sim.DefaultThreeNodeMemberSpec(),
		ElectionConfig:     sim.DefaultElectionConfig(),
		UseReplicationHost: true,
		MaxSteps:           20_000,
	}
	cfg.ElectionConfig.AppointedLeaderID = 0

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		// Elect appointed member 0 to LEADER_READY.
		require.NoError(t, sim.PumpUntilLeaderReady(s, 0, 15_000))
		leader := s.Node(0)
		leaderEl := leader.Election()
		require.Equal(t, election.ElectionStateLeaderReady, leaderEl.State())
		require.True(t, sim.HostIsSimReplicationHost(leader))
		agent := leader.AgentCM()
		require.NotNil(t, agent)

		term := leaderEl.LeadershipTermID()
		require.NotEqual(t, election.NullValue, term)

		// Raise leader append after READY.
		sim.SetLeaderAppend(leader, leaderAppend)
		require.Equal(t, leaderAppend, leaderEl.AppendPosition())
		src := agent.AppendSource()
		require.NotNil(t, src)
		got, ready := src.AppendPosition()
		require.True(t, ready)
		require.Equal(t, leaderAppend, got)

		// Drive NLT with high LogPosition; followers must not hang in CatchupInit.
		interval := cfg.ElectionConfig.LeaderHeartbeatIntervalNs
		require.NoError(t, sim.PumpNodes(s, interval, 20))
		for _, n := range s.Nodes() {
			el := n.Election()
			if el == nil {
				continue
			}
			require.NotEqual(
				t,
				election.ElectionStateFollowerCatchupInit,
				el.State(),
				"member %d stuck in CatchupInit",
				n.ID(),
			)
		}
		require.Equal(t, election.ElectionStateLeaderReady, leaderEl.State())

		// Inject follower appends into leader table; tick for commit publish.
		for _, fid := range []int32{1, 2} {
			require.NoError(t, sim.InjectAppendPosition(s, fid, 0, term, leaderAppend))
		}
		require.NoError(t, sim.PumpNodes(s, interval, 10))

		// Wire high-water at raised append proves inject + rank path (commit
		// is floor-bounded, so the max equals the raise exactly).
		require.Equal(
			t,
			leaderAppend,
			sim.WireCommitHighWater(leader, term),
			"expected CommitPosition logPosition=%d term=%d on wire",
			leaderAppend,
			term,
		)
		require.Equal(t, election.ElectionStateLeaderReady, leaderEl.State())
	})
}

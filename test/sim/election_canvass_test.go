package sim_test

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// canvassBaseConfig seeds a three-node simulation at the harness default
// starting positions. Scenarios that set AppointedLeaderID must supply a
// non-zero ElectionConfig, so the base is set explicitly rather than left zero
// for the harness to fill.
func canvassBaseConfig() sim.Config {
	return sim.Config{
		Seed:           42,
		MemberSpec:     sim.DefaultThreeNodeMemberSpec(),
		ElectionConfig: sim.DefaultElectionConfig(),
	}
}

func requireReaches(
	t *testing.T,
	s *sim.ElectionSim,
	nodeID int32,
	target election.ElectionState,
) {
	t.Helper()
	require.NoError(t, s.RunUntil(func(s *sim.ElectionSim) bool {
		return s.Node(nodeID).Election().State() == target
	}))
	require.Equalf(
		t,
		target,
		s.Node(nodeID).Election().State(),
		"node %d did not reach %v",
		nodeID,
		target,
	)
}

// requireVoteFor asserts the node published a TRUE ballot for candidateID.
func requireVoteFor(t *testing.T, node *sim.Node, candidateID int32) {
	t.Helper()
	for _, msg := range node.Publisher().Sent {
		if msg.TemplateID != uint16(clusterwire.VoteSbeTemplateID) {
			continue
		}
		vote := msg.Fields.(codecs.VoteFields)
		if vote.CandidateMemberID == candidateID {
			require.Equal(t, clusterwire.BooleanType_TRUE, vote.Vote)
			return
		}
	}
	require.Failf(t, "no vote published", "node published no vote for %d", candidateID)
}

// dropFollowerVote drops one follower's ballot so a candidate can only win by
// quorum after the election timeout, never unanimously.
type dropFollowerVote struct{ follower int32 }

func (d dropFollowerVote) Allow(
	from, _ int32,
	msg election.SentMessage,
	_ *rand.Rand,
) (bool, int64) {
	return from == d.follower && msg.TemplateID == uint16(clusterwire.VoteSbeTemplateID), 0
}

// dropCanvassToCandidate drops one follower's canvass position toward the
// candidate so the candidate canvasses a quorum but never unanimously.
type dropCanvassToCandidate struct{ follower, candidate int32 }

func (d dropCanvassToCandidate) Allow(
	from, to int32,
	msg election.SentMessage,
	_ *rand.Rand,
) (bool, int64) {
	drop := from == d.follower && to == d.candidate &&
		msg.TemplateID == uint16(clusterwire.CanvassPositionSbeTemplateID)
	return drop, 0
}

// TestSim_singleNode_leaderReady proves a lone member reaches LeaderReady
// without a ballot round.
func TestSim_singleNode_leaderReady(t *testing.T) {
	t.Parallel()

	cfg := canvassBaseConfig()
	cfg.MemberSpec = sim.DefaultSingleNodeMemberSpec()
	cfg.MaxSteps = 500

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		requireReaches(t, s, 0, election.ElectionStateLeaderReady)
		require.Equal(t, consensus.RoleLeader, s.Node(0).HostRole())
		require.Equal(t, int32(0), s.Node(0).Election().Leader().ID)
	})
}

func TestSim_appointedLeader_closed(t *testing.T) {
	t.Parallel()

	cfg := canvassBaseConfig()
	cfg.ElectionConfig.AppointedLeaderID = 0
	cfg.MaxSteps = 4000

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		requireReaches(t, s, 0, election.ElectionStateLeaderReady)
		require.Equal(t, consensus.RoleLeader, s.Node(0).HostRole())

		// The thin leader stub reaches READY without replicating its log to the
		// quorum, so inject the follower append position that closes the leader.
		leaderTerm := s.Node(0).Election().LeadershipTermID()
		s.Node(1).Publisher().AppendPosition(
			election.PublicationTarget{MemberID: 0},
			leaderTerm,
			0,
			1,
			0,
		)

		requireReaches(t, s, 0, election.ElectionStateClosed)
		require.Equal(t, int32(0), s.Node(0).Election().Leader().ID)
	})
}

func TestSim_voteForAppointedLeader_closed(t *testing.T) {
	t.Parallel()

	cfg := canvassBaseConfig()
	cfg.ElectionConfig.AppointedLeaderID = 0
	cfg.MaxSteps = 4000

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		requireReaches(t, s, 0, election.ElectionStateLeaderReady)
		requireReaches(t, s, 1, election.ElectionStateFollowerBallot)
		requireVoteFor(t, s.Node(1), 0)

		// The leader stub never publishes NewLeadershipTerm; inject it so the
		// follower runs the replay-through-ready close path.
		s.Node(0).Publisher().NewLeadershipTerm(
			election.PublicationTarget{MemberID: 1},
			codecs.NewLeadershipTermFields{
				LeadershipTermID: s.Node(0).Election().LeadershipTermID(),
				LeaderMemberID:   0,
				LogSessionID:     s.Node(0).Election().LogSessionID(),
			},
		)

		requireReaches(t, s, 1, election.ElectionStateClosed)
		require.Equal(t, consensus.RoleFollower, s.Node(1).HostRole())
		require.Equal(t, int32(0), s.Node(1).Election().Leader().ID)
	})
}

// TestSim_canvassToNominate_threeNode proves a full canvass exchange nominates
// unanimously, before any canvass deadline elapses.
func TestSim_canvassToNominate_threeNode(t *testing.T) {
	t.Parallel()

	cfg := canvassBaseConfig()
	cfg.MaxSteps = 2000

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		requireReaches(t, s, 1, election.ElectionStateNominate)
	})
}

func TestSim_canvassQuorum_nominatesAtHeartbeatDeadline(t *testing.T) {
	t.Parallel()

	// A non-startup candidate skips the extended canvass window and nominates on
	// the leader heartbeat deadline; dropping one follower's canvass position
	// forces the quorum path since unanimity needs every member reported.
	cfg := canvassBaseConfig()
	cfg.ElectionConfig.AppointedLeaderID = 1
	cfg.NetworkPolicy = dropCanvassToCandidate{follower: 2, candidate: 1}
	cfg.PerNodeOverrides = func(id int32, ec *election.ElectionConfig) {
		if id == 1 {
			ec.IsNodeStartup = false
		}
	}
	cfg.MaxSteps = 5000

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		deadline := s.Now() + cfg.ElectionConfig.LeaderHeartbeatTimeoutNs + 1

		// Drain the initial canvass exchange (RunUntil with a false predicate):
		// a quorum position alone must hold the candidate in CANVASS before the
		// heartbeat deadline.
		require.NoError(t, s.RunUntil(func(*sim.ElectionSim) bool { return false }))
		require.Equal(t, election.ElectionStateCanvass, s.Node(1).Election().State())

		// Advance past the deadline, then deliver node 0's periodic canvass
		// re-publish so the candidate re-evaluates the quorum.
		require.NoError(t, s.AdvanceTo(deadline))
		s.Node(0).Publisher().CanvassPosition(
			election.PublicationTarget{MemberID: 1},
			election.NullValue,
			0,
			election.NullValue,
			0,
		)

		requireReaches(t, s, 1, election.ElectionStateNominate)
	})
}

func TestSim_followerVotesOnRequest(t *testing.T) {
	t.Parallel()

	// Appoint node 0 so node 1 stays a follower: under the un-appointed base node
	// 1 nominates itself (that is the canvass-to-nominate scenario). The appointed
	// guard holds node 1 in CANVASS, and OnRequestVote's accept branch treats
	// CANVASS and NOMINATE alike, so it still votes TRUE and enters FOLLOWER_BALLOT.
	cfg := canvassBaseConfig()
	cfg.ElectionConfig.AppointedLeaderID = 0
	cfg.MaxSteps = 2000

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		requireReaches(t, s, 1, election.ElectionStateFollowerBallot)
		requireVoteFor(t, s.Node(1), 0)
		require.Equal(t, consensus.RoleFollower, s.Node(1).HostRole())
	})
}

func TestSim_candidateBallot_majorityWin(t *testing.T) {
	t.Parallel()

	cfg := canvassBaseConfig()
	cfg.ElectionConfig.AppointedLeaderID = 1
	cfg.NetworkPolicy = dropFollowerVote{follower: 2}
	cfg.PerNodeOverrides = func(id int32, ec *election.ElectionConfig) {
		if id == 1 {
			ec.IsNodeStartup = false
		}
	}
	cfg.MaxSteps = 5000

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		requireReaches(t, s, 1, election.ElectionStateCandidateBallot)

		// One follower's vote is dropped, so the ballot cannot win unanimously.
		// Advance past the election timeout, then deliver a message so the
		// candidate re-evaluates the ballot and takes the quorum path.
		deadline := s.Now() + cfg.ElectionConfig.ElectionTimeoutNs + 1
		require.NoError(t, s.AdvanceTo(deadline))
		s.Node(0).Publisher().CanvassPosition(
			election.PublicationTarget{MemberID: 1},
			0,
			0,
			0,
			0,
		)

		requireReaches(t, s, 1, election.ElectionStateLeaderLogReplication)
		require.Equal(t, consensus.RoleLeader, s.Node(1).HostRole())
		require.Equal(t, int32(1), s.Node(1).Election().Leader().ID)
	})
}

// TestSim_candidateBallot_unanimousWin proves an appointed leader wins the
// ballot on unanimous votes without waiting for election timeout.
func TestSim_candidateBallot_unanimousWin(t *testing.T) {
	t.Parallel()

	cfg := canvassBaseConfig()
	cfg.ElectionConfig.AppointedLeaderID = 1
	cfg.MaxSteps = 2000

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		requireReaches(t, s, 1, election.ElectionStateLeaderLogReplication)
		require.Equal(t, consensus.RoleLeader, s.Node(1).HostRole())
		require.Equal(t, int32(1), s.Node(1).Election().Leader().ID)
	})
}

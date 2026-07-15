package sim_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

func timeoutSimConfig() sim.Config {
	focus := int32(1)
	return sim.Config{
		Seed:           42,
		MemberSpec:     sim.DefaultThreeNodeMemberSpec(),
		ElectionConfig: sim.DefaultElectionConfig(),
		MaxSteps:       500,
		FocusNodeID:    &focus,
	}
}

func TestSim_timeoutCanvass_quorumMajority(t *testing.T) {
	t.Parallel()

	cfg := timeoutSimConfig()
	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		el := s.Node(1).Election()
		ecfg := cfg.ElectionConfig
		nullTerm := election.NullValue
		canvassDeadline := s.Now() + ecfg.StartupCanvassTimeoutNs

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		require.NoError(t, sim.InjectAppendPosition(s, 0, 1, nullTerm, 0))
		require.Equal(t, int64(0), election.MemberForTest(el, 0).LogPosition)
		require.Equal(t, election.NullPosition, election.MemberForTest(el, 2).LogPosition)

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		require.NoError(t, s.AdvanceToPhase(1, canvassDeadline-1))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		// Bare AdvanceTo runs no focus tick; the deadline fires only via
		// AdvanceToPhase.
		require.NoError(t, s.AdvanceTo(canvassDeadline+1))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		require.NoError(t, s.AdvanceToPhase(1, canvassDeadline+1))
		require.Equal(t, election.ElectionStateNominate, el.State())
	})
}

func TestSim_timeoutCandidateBallot_noQuorum(t *testing.T) {
	t.Parallel()

	cfg := timeoutSimConfig()
	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		el := s.Node(1).Election()
		ecfg := cfg.ElectionConfig
		nullTerm := election.NullValue

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		require.NoError(t, sim.InjectCanvassPosition(s, 0, 1, nullTerm, 0))
		require.NoError(t, sim.InjectCanvassPosition(s, 2, 1, nullTerm, 0))
		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.NoError(t, s.AdvanceToPhase(1, s.Now()+ecfg.ElectionTimeoutNs/2))
		require.Equal(t, election.ElectionStateCandidateBallot, el.State())
		ballotDeadline := s.Now() + ecfg.ElectionTimeoutNs

		require.NoError(t, s.AdvanceToPhase(1, ballotDeadline-1))
		require.Equal(t, election.ElectionStateCandidateBallot, el.State())

		require.NoError(t, s.AdvanceToPhase(1, ballotDeadline+1))
		require.Equal(t, election.ElectionStateCanvass, el.State())
		require.False(t, election.IsExtendedCanvassForTest(el))
		require.Equal(t, nullTerm, el.LeadershipTermID())
	})
}

func TestSim_splitVote_retryLeaderReady(t *testing.T) {
	t.Parallel()

	cfg := timeoutSimConfig()
	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		el := s.Node(1).Election()
		ecfg := cfg.ElectionConfig
		nullTerm := election.NullValue
		canvassDeadline := s.Now() + ecfg.StartupCanvassTimeoutNs

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		require.NoError(t, sim.InjectCanvassPosition(s, 0, 1, nullTerm, 0))
		require.NoError(t, s.AdvanceToPhase(1, canvassDeadline-1))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		require.NoError(t, s.AdvanceToPhase(1, canvassDeadline+1))
		require.Equal(t, election.ElectionStateNominate, el.State())

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+ecfg.ElectionTimeoutNs/2))
		require.Equal(t, election.ElectionStateCandidateBallot, el.State())
		require.Equal(t, nullTerm+1, election.CandidateTermIDForTest(el))
		firstBallotDeadline := s.Now() + ecfg.ElectionTimeoutNs

		require.NoError(t, sim.InjectVote(s, 2, 1, nullTerm+1, nullTerm, 0, false))
		require.NoError(t, s.AdvanceToPhase(1, s.Now()))
		require.Equal(t, election.ElectionStateCandidateBallot, el.State())
		require.NotNil(t, election.MemberForTest(el, 2).Vote)
		require.False(t, *election.MemberForTest(el, 2).Vote)
		require.Equal(t, nullTerm, election.MemberForTest(el, 2).LeadershipTermID)
		require.Equal(t, nullTerm+1, election.MemberForTest(el, 2).CandidateTermID)
		require.Nil(t, election.MemberForTest(s.Node(0).Election(), 2).Vote)

		require.NoError(t, s.AdvanceToPhase(1, firstBallotDeadline-1))
		require.Equal(t, election.ElectionStateCandidateBallot, el.State())

		require.NoError(t, s.AdvanceToPhase(1, firstBallotDeadline+1))
		require.Equal(t, election.ElectionStateCanvass, el.State())
		require.False(t, election.IsExtendedCanvassForTest(el))
		require.Equal(t, nullTerm, el.LeadershipTermID())

		require.NoError(t, sim.InjectCanvassPosition(s, 0, 1, nullTerm, 0))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+ecfg.LeaderHeartbeatTimeoutNs))
		require.Equal(t, election.ElectionStateNominate, el.State())

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+ecfg.ElectionTimeoutNs))
		require.Equal(t, election.ElectionStateCandidateBallot, el.State())
		require.Equal(t, nullTerm+2, election.CandidateTermIDForTest(el))
		secondBallotDeadline := s.Now() + ecfg.ElectionTimeoutNs

		require.NoError(t, sim.InjectVote(s, 2, 1, nullTerm+2, nullTerm+1, 0, true))
		require.NotNil(t, election.MemberForTest(el, 2).Vote)
		require.True(t, *election.MemberForTest(el, 2).Vote)
		require.Equal(t, nullTerm+1, election.MemberForTest(el, 2).LeadershipTermID)
		require.Equal(t, nullTerm+2, election.MemberForTest(el, 2).CandidateTermID)

		require.NoError(t, s.AdvanceToPhase(1, secondBallotDeadline-1))
		require.Equal(t, election.ElectionStateCandidateBallot, el.State())

		require.NoError(t, s.AdvanceToPhase(1, secondBallotDeadline+1))
		require.Equal(t, election.ElectionStateLeaderLogReplication, el.State())

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.Equal(t, election.ElectionStateLeaderReplay, el.State())

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.Equal(t, election.ElectionStateLeaderInit, el.State())

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.Equal(t, election.ElectionStateLeaderReady, el.State())
		require.Equal(t, nullTerm+2, el.LeadershipTermID())
		require.Equal(t, consensus.RoleLeader, s.Node(1).HostRole())
	})
}

func TestSim_timeoutFollowerBallot(t *testing.T) {
	t.Parallel()

	cfg := timeoutSimConfig()
	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		el := s.Node(1).Election()
		ecfg := cfg.ElectionConfig
		nullTerm := election.NullValue
		candidateTerm := nullTerm + 1

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+1))
		require.Equal(t, election.ElectionStateCanvass, el.State())

		require.NoError(t, sim.InjectRequestVote(s, 0, 1, candidateTerm, nullTerm, 0))
		require.Equal(t, election.ElectionStateFollowerBallot, el.State())

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+ecfg.ElectionTimeoutNs-2))
		require.Equal(t, election.ElectionStateFollowerBallot, el.State())

		require.NoError(t, s.AdvanceToPhase(1, s.Now()+2))
		require.Equal(t, election.ElectionStateCanvass, el.State())
		require.Equal(t, nullTerm, el.LeadershipTermID())
	})
}

func TestSim_becomeFollowerOnNewElection(t *testing.T) {
	t.Parallel()

	cfg := timeoutSimConfig()
	focus := int32(0)
	cfg.FocusNodeID = &focus
	cfg.PerNodeOverrides = func(id int32, ec *election.ElectionConfig) {
		if id == 0 {
			ec.IsNodeStartup = false
			ec.LeadershipTermID = 1
			ec.LogPosition = 120
			ec.AppendPosition = 120
		}
	}

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		s.Node(0).SetHostRole(consensus.RoleLeader)

		start := s.Now()
		require.NoError(t, s.AdvanceToPhase(0, start+1))

		el := s.Node(0).Election()
		snap := s.Node(0).Snapshot()
		require.Equal(t, election.ElectionStateCanvass, el.State())
		require.Equal(t, 1, snap.PrepareCalls)
		require.Equal(t, int64(120), snap.PrepareLogPosition)
		require.Equal(t, start, snap.PrepareNowNs)
		require.Equal(t, int64(120), el.AppendPosition())
		require.Equal(t, consensus.RoleFollower, s.Node(0).HostRole())
	})
}

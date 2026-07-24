//go:build testhooks

package sim

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/election"
)

// Signal decrease for same (member, term) fails CheckCommitMonotonic.
func TestCheckCommitMonotonic_regression(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(901), func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		n := s.Node(0)
		term := n.election.LeadershipTermID()
		s.lastCommitSignals = map[commitSignalKey]int64{
			{memberID: n.id, term: term}: 1000,
		}

		// Live signal is 0 (no wire CommitPosition, agent not READY or 0).
		err := CheckCommitMonotonic(s)
		require.ErrorContains(t, err, "commit regression")
	})
}

// Advance above model floor fails CheckCommitBoundedByModelQuorum.
func TestCheckCommitBoundedByModelQuorum_advanceAboveFloor(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(902), func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		n := s.Node(0)
		election.SetStateForTest(n.election, election.ElectionStateLeaderReady)
		term := n.election.LeadershipTermID()

		// Floor: members at log 0; append low.
		SetLeaderAppend(n, 100)
		setMembersLogStamp(n, 0, 0)

		// Publish CommitPosition above floor without recording last signal.
		publishCommitPosition(n, term, 500, n.id)
		s.lastCommitSignals = map[commitSignalKey]int64{}

		err := CheckCommitBoundedByModelQuorum(s)
		require.ErrorContains(t, err, "commit advance above floor")
	})
}

// On-advance only: unchanged signal does not re-check floor even when live
// floor would be below signal.
func TestCheckCommitBoundedByModelQuorum_unchangedSkipsFloor(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(903), func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		n := s.Node(0)
		election.SetStateForTest(n.election, election.ElectionStateLeaderReady)
		term := n.election.LeadershipTermID()

		const signal int64 = 1000
		publishCommitPosition(n, term, signal, n.id)
		s.lastCommitSignals = map[commitSignalKey]int64{
			{memberID: n.id, term: term}: signal,
		}

		// Drop floor: zero stamps + low append so floor is 0.
		SetLeaderAppend(n, 0)
		setMembersLogStamp(n, 0, 0)
		floor := ModelQuorumFloor(n, s.Now())
		require.Less(t, floor, signal, "precondition: floor below sticky signal")

		require.NoError(t, CheckCommitBoundedByModelQuorum(s))
		require.NoError(t, CheckCommitMonotonic(s))
	})
}

// Two live self-leaders fail CheckSingleCommitAuthority.
func TestCheckSingleCommitAuthority_dualSelfLeaders(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(904), func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		forceTwoLiveSelfLeaders(s)
		err := CheckSingleCommitAuthority(s)
		require.ErrorContains(t, err, "two live self-leaders")
	})
}

// Distinct LeaderMemberID on CommitPosition for same term fails.
func TestCheckSingleCommitAuthority_dualPublishers(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(905), func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		n0 := s.Node(0)
		n1 := s.Node(1)
		election.SetStateForTest(n0.election, election.ElectionStateLeaderReady)

		// Only n0 self-leader for the dual-publisher path. Both self-leaders
		// would hit split-brain first. One self-leader with two LeaderMemberIDs
		// on its Sent is dual commit authority on wire.
		election.SetStateForTest(n1.election, election.ElectionStateCanvass)
		term := n0.election.LeadershipTermID()
		publishCommitPosition(n0, term, 100, n0.id)
		publishCommitPosition(n0, term, 100, n1.id)

		err := CheckSingleCommitAuthority(s)
		require.ErrorContains(t, err, "dual commit authority")
	})
}

// Happy path: UseReplicationHost continuous commit safety quiet after elect + inject.
func TestReplicationCommitSafety_happyPathQuiet(t *testing.T) {
	t.Parallel()

	const leaderAppend int64 = 10_000

	cfg := Config{
		Seed:               906,
		MemberSpec:         DefaultThreeNodeMemberSpec(),
		ElectionConfig:     DefaultElectionConfig(),
		UseReplicationHost: true,
		MaxSteps:           20_000,
	}
	cfg.ElectionConfig.AppointedLeaderID = 0

	Run(t, cfg, func(s *ElectionSim) {
		require.NoError(t, PumpUntilLeaderReady(s, 0, 15_000))
		leader := s.Node(0)
		require.Equal(t, election.ElectionStateLeaderReady, leader.election.State())
		term := leader.election.LeadershipTermID()

		SetLeaderAppend(leader, leaderAppend)
		require.Equal(t, leaderAppend, LeaderAppend(leader))

		interval := cfg.ElectionConfig.LeaderHeartbeatIntervalNs
		require.NoError(t, PumpNodes(s, interval, 20))

		for _, fid := range []int32{1, 2} {
			require.NoError(t, InjectAppendPosition(s, fid, 0, term, leaderAppend))
		}
		require.NoError(t, PumpNodes(s, interval, 10))

		require.NoError(t, s.SafetyErr())
		require.NoError(t, CheckCommitMonotonic(s))
		require.NoError(t, CheckCommitBoundedByModelQuorum(s))
		require.NoError(t, CheckSingleCommitAuthority(s))
		require.Positive(t, WireCommitHighWater(leader, term))
	})
}

// SkipReplicationSafety: continuous does not record commit safety when skip true.
func TestReplicationCommitSafety_skipDisablesContinuous(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(907), func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		n := s.Node(0)
		term := n.election.LeadershipTermID()

		// Set a commit regression that continuous would catch if enabled.
		s.lastCommitSignals = map[commitSignalKey]int64{
			{memberID: n.id, term: term}: 9999,
		}
		require.NoError(t, checkContinuousSafety(s))
		require.NoError(t, s.SafetyErr())

		// Direct monotonic helper still fails (always available).
		require.Error(t, CheckCommitMonotonic(s))
	})
}

// Default election mode (UseReplicationHost false): continuous never runs
// commit safety checks.
func TestReplicationCommitSafety_defaultElectionModeUnaffected(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Seed:                908,
		SkipSplitBrainCheck: true,
		ElectionConfig:      DefaultElectionConfig(),
	}
	Run(t, cfg, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))
		n := s.Node(0)
		term := n.election.LeadershipTermID()

		// lastCommitSignals stays nil without replication continuous path.
		require.Nil(t, s.lastCommitSignals)

		// Force a regression signal; continuous must not touch commit checks.
		s.lastCommitSignals = map[commitSignalKey]int64{
			{memberID: n.id, term: term}: 9999,
		}
		require.NoError(t, checkContinuousSafety(s))
		require.NoError(t, s.SafetyErr())

		// Map not advanced by continuous (no recordCommitSignals).
		require.Equal(t, int64(9999), s.lastCommitSignals[commitSignalKey{
			memberID: n.id,
			term:     term,
		}])
	})
}

// WireCommitHighWater takes max LogPosition for matching term.
func TestWireCommitHighWater(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(909), func(s *ElectionSim) {
		n := s.Node(0)
		require.Equal(t, int64(0), WireCommitHighWater(n, 1))

		publishCommitPosition(n, 1, 10, n.id)
		publishCommitPosition(n, 1, 50, n.id)
		publishCommitPosition(n, 2, 99, n.id)
		require.Equal(t, int64(50), WireCommitHighWater(n, 1))
		require.Equal(t, int64(99), WireCommitHighWater(n, 2))
	})
}

// ModelQuorumFloor is min(quorum, append) on leader members.
func TestModelQuorumFloor(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(910), func(s *ElectionSim) {
		n := s.Node(0)
		now := s.Now()

		// All members active with high log; append lower bounds floor.
		SetLeaderAppend(n, 100)
		setMembersLogStamp(n, 1000, now)
		require.Equal(t, int64(100), ModelQuorumFloor(n, now))

		// Raise append above quorum of active logs.
		SetLeaderAppend(n, 5000)
		setMembersLogStamp(n, 200, now)

		// 3 members, threshold 2: ranked floor is 2nd highest = 200.
		require.Equal(t, int64(200), ModelQuorumFloor(n, now))
	})
}

// Continuous path records last signals after a quiet step under replication.
func TestReplicationCommitSafety_continuousRecordsWatermark(t *testing.T) {
	t.Parallel()

	Run(t, continuousReplicationConfig(911), func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		n := s.Node(0)
		election.SetStateForTest(n.election, election.ElectionStateLeaderReady)
		term := n.election.LeadershipTermID()

		// Safe advance: floor at least as high as signal.
		setFloorAndPublishCommit(n, term, 500, 200, s.Now())

		require.NoError(t, checkContinuousSafety(s))
		require.NoError(t, s.SafetyErr())
		require.Equal(t, int64(200), s.lastCommitSignals[commitSignalKey{
			memberID: n.id,
			term:     term,
		}])
	})
}

// Crash clears per-member commit watermarks so restart under the restored term
// with empty Sent / fresh Agent does not false-fail commit regression.
func TestReplicationCommitSafety_crashClearsWatermark(t *testing.T) {
	t.Parallel()

	Run(t, continuousReplicationConfig(913), func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		n := s.Node(0)
		election.SetStateForTest(n.election, election.ElectionStateLeaderReady)
		term := n.election.LeadershipTermID()
		setFloorAndPublishCommit(n, term, 500, 200, s.Now())

		require.NoError(t, checkContinuousSafety(s))
		require.Equal(t, int64(200), s.lastCommitSignals[commitSignalKey{
			memberID: n.id,
			term:     term,
		}])

		require.NoError(t, s.CrashNode(0))
		_, still := s.lastCommitSignals[commitSignalKey{memberID: n.id, term: term}]
		require.False(t, still, "crash must drop watermarks for crashed member")

		require.NoError(t, s.RestartNode(0))
		restored := s.Node(0)
		require.NotNil(t, restored.election)

		// Restored term, empty wire, agent 0: continuous must stay quiet.
		require.Equal(t, term, restored.election.LeadershipTermID())
		require.Equal(t, int64(0), WireCommitHighWater(restored, term))
		require.NoError(t, checkContinuousSafety(s))
		require.NoError(t, s.SafetyErr())
	})
}

// N=1 uses Agent commit under LEADER_READY; wire Sent stays empty.
func TestReplicationCommitSafety_singleNodeAgentSignal(t *testing.T) {
	t.Parallel()

	cfg := continuousReplicationConfig(914)
	cfg.MemberSpec = DefaultSingleNodeMemberSpec()
	Run(t, cfg, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		n := s.Node(0)
		election.SetStateForTest(n.election, election.ElectionStateLeaderReady)
		term := n.election.LeadershipTermID()

		const agentCommit int64 = 300
		SetLeaderAppend(n, agentCommit)
		setMembersLogStamp(n, agentCommit, s.Now())
		n.AgentCM().ProposeMaxCommit(agentCommit)

		require.Equal(t, int64(0), WireCommitHighWater(n, term))
		require.Equal(t, agentCommit, AgentCommitPosition(n))

		require.NoError(t, CheckCommitMonotonic(s))
		require.NoError(t, CheckCommitBoundedByModelQuorum(s))
		require.NoError(t, checkContinuousSafety(s))
		require.NoError(t, s.SafetyErr())
		require.Equal(t, agentCommit, s.lastCommitSignals[commitSignalKey{
			memberID: n.id,
			term:     term,
		}])
	})
}

// AgentCommitPosition and LeaderAppend accessors.
func TestLeaderAppendAndAgentCommitPosition(t *testing.T) {
	t.Parallel()

	Run(t, skipReplicationSafetyConfig(912), func(s *ElectionSim) {
		n := s.Node(0)
		require.Equal(t, int64(0), LeaderAppend(n))
		require.Equal(t, int64(0), AgentCommitPosition(n))

		SetLeaderAppend(n, 42)
		require.Equal(t, int64(42), LeaderAppend(n))

		require.NotNil(t, n.AgentCM())
		n.AgentCM().ProposeMaxCommit(77)
		require.Equal(t, int64(77), AgentCommitPosition(n))

		require.Equal(t, int64(0), LeaderAppend(nil))
		require.Equal(t, int64(0), AgentCommitPosition(nil))
	})
}

// skipReplicationSafetyConfig builds a UseReplicationHost config that skips
// continuous commit safety (and split-brain) so tests can force illegal state.
func skipReplicationSafetyConfig(seed uint64) Config {
	return Config{
		Seed:                  seed,
		UseReplicationHost:    true,
		SkipReplicationSafety: true,
		SkipSplitBrainCheck:   true,
		ElectionConfig:        DefaultElectionConfig(),
	}
}

// continuousReplicationConfig is UseReplicationHost with continuous commit
// safety on. Split-brain check off for forced election/wire state.
func continuousReplicationConfig(seed uint64) Config {
	return Config{
		Seed:                seed,
		UseReplicationHost:  true,
		SkipSplitBrainCheck: true,
		ElectionConfig:      DefaultElectionConfig(),
	}
}

func setMembersLogStamp(n *Node, logPos, stampNs int64) {
	for _, m := range election.MembersForTest(n.election) {
		m.LogPosition = logPos
		m.TimeOfLastAppendPositionNs = stampNs
	}
}

// setFloorAndPublishCommit raises append and member logs so floor is at least
// floorPos, then publishes a wire CommitPosition at signal.
func setFloorAndPublishCommit(n *Node, term, floorPos, signal, nowNs int64) {
	SetLeaderAppend(n, floorPos)
	setMembersLogStamp(n, floorPos, nowNs)
	publishCommitPosition(n, term, signal, n.id)
}

func publishCommitPosition(n *Node, term, logPos int64, leaderMemberID int32) {
	n.publisher.CommitPosition(
		election.PublicationTarget{MemberID: leaderMemberID ^ 1},
		codecs.CommitPositionFields{
			LeadershipTermID: term,
			LogPosition:      logPos,
			LeaderMemberID:   leaderMemberID,
		},
	)
}

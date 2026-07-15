package election_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

func TestSnapshotForTest_capturesObservableState(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	pub := deps.Publisher.(*election.SbePublisher)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCandidateBallot)
	pub.RequestVote(election.PublicationTarget{MemberID: 1}, 1, 2, 3, 4)

	snap := election.SnapshotForTest(e, consensus.RoleCandidate, pub)

	require.Equal(t, election.ElectionStateCandidateBallot, snap.State)
	require.Equal(t, consensus.RoleCandidate, snap.Role)
	require.Equal(t, cfg.LeadershipTermID, snap.LeadershipTermID)
	require.Equal(t, cfg.LeadershipTermID, snap.CandidateTermID)
	require.Len(t, snap.MemberSnapshots, 3)
	require.Equal(t, int32(0), snap.MemberSnapshots[0].ID)
	require.Equal(t, 1, snap.PublishedCount)
}

func TestElection_DoWork_closedReturnsZero(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	election.SetStateForTest(e, election.ElectionStateClosed)
	require.Equal(t, 0, e.DoWork(1_000))
}

func TestElection_DoWork_stubStatesReturnZero(t *testing.T) {
	t.Parallel()

	states := []election.ElectionState{
		election.ElectionStateFollowerCatchupInit,
		election.ElectionStateFollowerCatchupAwait,
		election.ElectionStateFollowerCatchup,
	}
	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, state)

			require.Equal(t, 0, e.DoWork(clock.nowNs))
			require.Equal(t, state, e.State())
		})
	}
}

func TestElection_DoWork_leaderLogReplicationReturnsWork(t *testing.T) {
	t.Parallel()

	fx := newElectionFixture(t, withSingleNode(), withPositions(0, 0, 0))

	fx.doWork()
	require.Equal(t, election.ElectionStateLeaderLogReplication, fx.election.State())

	// Quorum equals the append position, so one duty cycle publishes and
	// advances to replay.
	require.Positive(t, fx.doWork())
	require.Equal(t, election.ElectionStateLeaderReplay, fx.election.State())
}

func TestElection_DoWork_panicsOnUnknownState(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	election.SetStateForTest(e, election.ElectionState(99))
	require.PanicsWithValue(t, "election: unknown state ElectionState(99)", func() {
		e.DoWork(1_000)
	})
}

func TestNewElection_initializesFields(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = true
	cfg.GracefulClosedLeaderID = 7
	observer := &recordingStateObserver{}
	deps.StateObserver = observer

	e := election.NewElection(cfg, deps)

	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, cfg.LeadershipTermID, e.LeadershipTermID())
	require.Equal(t, cfg.LogPosition, e.LogPosition())
	require.Equal(t, cfg.AppendPosition, e.AppendPosition())
	require.Equal(t, cfg.MemberID, e.ThisMemberID())
	require.Nil(t, e.Leader())
	require.Equal(t, election.NullMemberID, e.LogSessionID())
	require.Equal(t, int64(0), e.NotifiedCommitPosition())
	require.Equal(t, []election.ElectionState{election.ElectionStateInit}, observer.states)

	initialUpdate := clock.nowNs - int64(24*time.Hour)
	require.Equal(t, initialUpdate, election.TimeOfLastUpdateNsForTest(e))
	require.Equal(t, initialUpdate, election.TimeOfLastCommitPositionUpdateNsForTest(e))
}

func TestElectionConfig_defaults(t *testing.T) {
	t.Parallel()

	require.Equal(t, election.DefaultElectionConfig(), election.ElectionTestConfig())

	cfg := election.DefaultElectionConfig()
	require.Equal(t, int64(1_000_000_000), cfg.ElectionTimeoutNs)
	require.Equal(t, int64(100_000_000), cfg.ElectionStatusIntervalNs)
	require.Equal(t, int64(60_000_000_000), cfg.StartupCanvassTimeoutNs)
	require.Equal(t, int64(10_000_000_000), cfg.LeaderHeartbeatTimeoutNs)
	require.Equal(t, int64(200_000_000), cfg.LeaderHeartbeatIntervalNs)
	require.NoError(t, election.ValidateElectionConfig(cfg))
}

func TestValidateElectionConfig_rejectsZeroTimeouts(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		mut  func(*election.ElectionConfig)
		msg  string
	}{
		{
			"zero ElectionTimeoutNs",
			func(c *election.ElectionConfig) { c.ElectionTimeoutNs = 0 },
			"ElectionTimeoutNs must be positive",
		},
		{
			"zero ElectionStatusIntervalNs",
			func(c *election.ElectionConfig) { c.ElectionStatusIntervalNs = 0 },
			"ElectionStatusIntervalNs must be positive",
		},
		{
			"zero LeaderHeartbeatTimeoutNs",
			func(c *election.ElectionConfig) { c.LeaderHeartbeatTimeoutNs = 0 },
			"LeaderHeartbeatTimeoutNs must be positive",
		},
		{
			"zero StartupCanvassTimeoutNs",
			func(c *election.ElectionConfig) { c.StartupCanvassTimeoutNs = 0 },
			"StartupCanvassTimeoutNs must be positive",
		},
		{
			"zero LeaderHeartbeatIntervalNs",
			func(c *election.ElectionConfig) { c.LeaderHeartbeatIntervalNs = 0 },
			"LeaderHeartbeatIntervalNs must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := election.DefaultElectionConfig()
			tc.mut(&cfg)
			err := election.ValidateElectionConfig(cfg)
			require.EqualError(t, err, "election: "+tc.msg)
		})
	}
}

func TestNewElection_rejectsZeroTimeout(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	cfg.ElectionTimeoutNs = 0

	require.PanicsWithValue(t, "election: ElectionTimeoutNs must be positive", func() {
		election.NewElection(cfg, deps)
	})
}

func TestNewElection_validationPanics(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)

	panics := []struct {
		name string
		mut  func(*election.ElectionDeps)
		msg  string
	}{
		{"nil Handler", func(d *election.ElectionDeps) { d.Handler = nil }, "nil Handler"},
		{"nil Host", func(d *election.ElectionDeps) { d.Host = nil }, "nil Host"},
		{"nil Publisher", func(d *election.ElectionDeps) { d.Publisher = nil }, "nil Publisher"},
		{"nil NanoClock", func(d *election.ElectionDeps) { d.NanoClock = nil }, "nil NanoClock"},
		{"nil EpochClock", func(d *election.ElectionDeps) { d.EpochClock = nil }, "nil EpochClock"},
		{"nil NodeState", func(d *election.ElectionDeps) { d.NodeState = nil }, "nil NodeState"},
		{"nil ThisMember", func(d *election.ElectionDeps) { d.ThisMember = nil }, "nil ThisMember"},
		{"empty Members", func(d *election.ElectionDeps) { d.Members = nil }, "empty Members"},
		{
			"duplicate member id",
			func(d *election.ElectionDeps) {
				m := &election.Member{ID: 0}
				d.Members = []*election.Member{m, m}
			},
			"duplicate member id",
		},
		{
			"ThisMember not in Members",
			func(d *election.ElectionDeps) { d.ThisMember = &election.Member{ID: 99} },
			"ThisMember not in Members",
		},
	}

	cfgMismatch := cfg
	cfgMismatch.MemberID = 99
	require.PanicsWithValue(t, "election: MemberID mismatch", func() {
		election.NewElection(cfgMismatch, deps)
	})

	for _, tc := range panics {
		t.Run(tc.name, func(t *testing.T) {
			d := deps
			tc.mut(&d)
			require.PanicsWithValue(t, "election: "+tc.msg, func() {
				election.NewElection(cfg, d)
			})
		})
	}
}

func TestElection_shouldElectSingleNodeClusterLeader(t *testing.T) {
	t.Parallel()

	fx := newElectionFixture(t, withSingleNode(), withPositions(0, 0, 0))
	fx.driveUntilState(election.ElectionStateLeaderReady, 10)

	require.Equal(t, consensus.RoleLeader, fx.host.role)
	require.Len(t, fx.host.addLogPublicationCalls, 1)
	require.Equal(t, fixtureLogSessionID, fx.election.LogSessionID())

	require.Len(t, fx.agent.joinLeaderCalls, 1)
	require.Equal(t, int64(1), fx.agent.joinLeaderCalls[0].leadershipTermID)
	require.Equal(t, fixtureLogSessionID, fx.agent.joinLeaderCalls[0].logSessionID)

	fx.requireLocalLeader()
	require.Equal(t, int64(1), fx.election.LeadershipTermID())
}

func TestElection_shouldElectAppointedLeader(t *testing.T) {
	t.Parallel()

	fx := newElectionFixture(t, withAppointedLeaderID(0))

	fx.canvassTo(election.ElectionStateCandidateBallot)

	fx.requireRequestVoteBroadcast()
	fx.injectVote(1, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)
	fx.injectVote(2, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)
	fx.driveUntilState(election.ElectionStateLeaderReady, 20)
	require.Equal(t, consensus.RoleLeader, fx.host.role)

	// Follower append positions only update member state; closure happens on
	// the next LEADER_READY duty cycle once a quorum has reached the position.
	fx.injectAppendPosition(1, fx.election.LeadershipTermID(), fx.cfg.AppendPosition)
	require.Equal(t, election.ElectionStateLeaderReady, fx.election.State())
	fx.injectAppendPosition(2, fx.election.LeadershipTermID(), fx.cfg.AppendPosition)
	require.Equal(t, election.ElectionStateLeaderReady, fx.election.State())

	fx.doWork()

	require.Equal(t, election.ElectionStateClosed, fx.election.State())
	require.Len(t, fx.host.appendNewLeadershipTermEventCalls, 1)
	require.Len(t, fx.agent.electionCompleteNs, 1)
	fx.requireLocalLeader()
}

func TestElection_shouldVoteForAppointedLeader(t *testing.T) {
	t.Parallel()

	// Log and append positions match, as in the Java oracle: the follower joins
	// the live log with nothing to replay.
	fx := newElectionFixture(
		t,
		withAppointedLeaderID(1),
		withPositions(testLeadershipTermID, testLogPosition, testLogPosition),
	)

	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.injectRequestVote(1, 6, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)
	fx.requireVoteSentFor(1)

	fx.injectNewLeadershipTerm(1, 6)
	require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())

	fx.driveUntilState(election.ElectionStateClosed, 20)

	require.Equal(t, consensus.RoleFollower, fx.host.role)
	require.Equal(t, 1, fx.agent.joinFollowerCalls)
	require.Len(t, fx.agent.electionCompleteNs, 1)
	require.NotNil(t, fx.election.Leader())
	require.Equal(t, int32(1), fx.election.Leader().ID)
	require.Equal(t, int64(6), fx.election.LeadershipTermID())
	require.Equal(t, fixtureLogSessionID, fx.election.LogSessionID())
}

func TestElection_shouldCanvassMembersInSuccessfulLeadershipBid(t *testing.T) {
	t.Parallel()

	fx := newElectionFixture(t)

	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.publisher.Reset()
	fx.doWork()
	require.Len(t, fx.publisher.Sent, 2)
	targets := map[int32]struct{}{}
	for _, msg := range fx.publisher.Sent {
		require.Equal(t, uint16(clusterwire.CanvassPositionSbeTemplateID), msg.TemplateID)
		targets[msg.Dst.MemberID] = struct{}{}
	}
	require.Equal(t, map[int32]struct{}{1: {}, 2: {}}, targets)
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.injectBaselineCanvassPositions()
	require.Equal(t, fx.cfg.AppendPosition, fx.member(1).LogPosition)
	require.Equal(t, fx.cfg.AppendPosition, fx.member(2).LogPosition)

	fx.driveUntilState(election.ElectionStateNominate, 20)
}

func TestElection_shouldVoteForCandidateDuringNomination(t *testing.T) {
	t.Parallel()

	fx := newElectionFixture(t)

	fx.canvassTo(election.ElectionStateNominate)

	fx.publisher.Reset()
	fx.injectRequestVote(1, 7, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)
	fx.requireVoteSentFor(1)
}

func TestElection_shouldWinCandidateBallotWithMajority(t *testing.T) {
	t.Parallel()

	fx := newElectionFixture(t, withNodeStartup(false))

	fx.canvassTo(election.ElectionStateCandidateBallot)
	require.Equal(t, 1, fx.agent.prepareCalls)

	fx.requireRequestVoteBroadcast()

	fx.injectVote(1, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)

	fx.advance(fx.cfg.ElectionTimeoutNs)
	fx.doWork()

	require.Equal(t, election.ElectionStateLeaderLogReplication, fx.election.State())
	require.Equal(t, consensus.RoleLeader, fx.host.role)
	fx.requireLocalLeader()
}

func TestElection_shouldElectCandidateWithFullVote(t *testing.T) {
	t.Parallel()

	fx := newElectionFixture(t)

	fx.canvassTo(election.ElectionStateCandidateBallot)

	// Nomination proposes candidateTermID+1 once through the monotonic node state.
	require.Len(t, fx.nodeState.proposeCalls, 1)
	require.Equal(t, fx.cfg.LeadershipTermID+1, fx.nodeState.proposeCalls[0].candidateTermID)
	require.Equal(t, fx.cfg.LogPosition, fx.nodeState.proposeCalls[0].logPosition)
	require.Equal(t, fx.cfg.LeadershipTermID+1, election.CandidateTermIDForTest(fx.election))

	fx.requireRequestVoteBroadcast()

	fx.injectVote(1, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)
	fx.injectVote(2, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)
	fx.doWork()

	require.Equal(t, election.ElectionStateLeaderLogReplication, fx.election.State())
	require.Equal(t, consensus.RoleLeader, fx.host.role)
	fx.requireLocalLeader()
}

func TestElection_shouldTimeoutCanvassWithMajority(t *testing.T) {
	t.Parallel()

	fx := newTimeoutElection(t, 1)

	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.injectAppendPosition(0, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)
	require.Equal(t, fx.cfg.AppendPosition, fx.member(0).LogPosition)
	require.Equal(t, election.NullPosition, fx.member(2).LogPosition)

	fx.advance(1)
	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.advance(fx.cfg.StartupCanvassTimeoutNs - 2)
	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.advance(2)
	fx.doWork()

	require.Equal(t, election.ElectionStateNominate, fx.election.State())
	require.Equal(t, []election.ElectionState{
		election.ElectionStateInit,
		election.ElectionStateCanvass,
		election.ElectionStateNominate,
	}, fx.observer.states)
}

func TestElection_shouldTimeoutCandidateBallotWithoutMajority(t *testing.T) {
	t.Parallel()

	fx := newTimeoutElection(t, 1)

	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.injectCanvassPosition(0, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)
	fx.injectCanvassPosition(2, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)

	fx.advance(1)
	fx.doWork()
	require.Equal(t, election.ElectionStateNominate, fx.election.State())

	// NominationRand=0 collapses nominationDelayNs to zero; the half-timeout step
	// still mirrors Java's clock increment before the first post-nominate DoWork.
	fx.advance(fx.cfg.ElectionTimeoutNs >> 1)
	fx.doWork()
	require.Equal(t, election.ElectionStateCandidateBallot, fx.election.State())

	fx.advance(fx.cfg.ElectionTimeoutNs - 1)
	fx.doWork()
	require.Equal(t, election.ElectionStateCandidateBallot, fx.election.State())

	fx.advance(1)
	fx.doWork()

	require.Equal(t, election.ElectionStateCanvass, fx.election.State())
	require.False(t, election.IsExtendedCanvassForTest(fx.election))
	require.Equal(t, election.NullValue, fx.election.LeadershipTermID())
	require.Equal(t, []election.ElectionState{
		election.ElectionStateInit,
		election.ElectionStateCanvass,
		election.ElectionStateNominate,
		election.ElectionStateCandidateBallot,
		election.ElectionStateCanvass,
	}, fx.observer.states)
}

func TestElection_shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry(t *testing.T) {
	t.Parallel()

	fx := newTimeoutElection(t, 1)

	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.injectCanvassPosition(0, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)

	fx.advance(fx.cfg.StartupCanvassTimeoutNs - 2)
	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fx.advance(2)
	fx.doWork()
	require.Equal(t, election.ElectionStateNominate, fx.election.State())

	fx.advance(fx.cfg.ElectionTimeoutNs >> 1)
	fx.doWork()
	require.Equal(t, election.ElectionStateCandidateBallot, fx.election.State())

	require.Equal(t, election.NullValue+1, election.CandidateTermIDForTest(fx.election))
	fx.injectVote(2, election.NullValue, fx.cfg.AppendPosition, false)
	fx.doWork()
	require.Equal(t, election.ElectionStateCandidateBallot, fx.election.State())
	require.NotNil(t, fx.member(2).Vote)
	require.False(t, *fx.member(2).Vote)
	require.Equal(t, election.NullValue, fx.member(2).LeadershipTermID)
	require.Equal(t, election.NullValue+1, fx.member(2).CandidateTermID)

	fx.advance(fx.cfg.ElectionTimeoutNs - 1)
	fx.doWork()
	require.Equal(t, election.ElectionStateCandidateBallot, fx.election.State())

	fx.advance(1)
	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())
	require.False(t, election.IsExtendedCanvassForTest(fx.election))
	require.Equal(t, election.NullValue, fx.election.LeadershipTermID())

	fx.injectCanvassPosition(0, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)

	// Noop host returns TimeOfLastLeaderUpdateNs() == 0, so the re-canvass
	// deadline is anchored at epoch 0 rather than the last leader heartbeat.
	fx.advance(fx.cfg.LeaderHeartbeatTimeoutNs)
	fx.doWork()
	require.Equal(t, election.ElectionStateNominate, fx.election.State())

	fx.advance(fx.cfg.ElectionTimeoutNs)
	fx.doWork()
	require.Equal(t, election.ElectionStateCandidateBallot, fx.election.State())

	require.Equal(t, election.NullValue+2, election.CandidateTermIDForTest(fx.election))
	fx.injectVote(2, election.NullValue+1, fx.cfg.AppendPosition, true)
	require.NotNil(t, fx.member(2).Vote)
	require.True(t, *fx.member(2).Vote)
	require.Equal(t, election.NullValue+1, fx.member(2).LeadershipTermID)
	require.Equal(t, election.NullValue+2, fx.member(2).CandidateTermID)

	fx.advance(fx.cfg.ElectionTimeoutNs - 1)
	fx.doWork()
	require.Equal(t, election.ElectionStateCandidateBallot, fx.election.State())

	fx.advance(1)
	fx.doWork()
	require.Equal(t, election.ElectionStateLeaderLogReplication, fx.election.State())

	fx.doWork()
	require.Equal(t, election.ElectionStateLeaderReplay, fx.election.State())

	fx.doWork()
	require.Equal(t, election.ElectionStateLeaderInit, fx.election.State())

	fx.doWork()
	require.Equal(t, election.ElectionStateLeaderReady, fx.election.State())
	require.Equal(t, election.NullValue+2, fx.election.LeadershipTermID())
	require.Equal(t, consensus.RoleLeader, fx.host.role)
	fx.requireLocalLeader()
	require.Len(t, fx.agent.joinLeaderCalls, 1)
	require.Equal(t, election.NullValue+2, fx.agent.joinLeaderCalls[0].leadershipTermID)
	require.Equal(t, fixtureLogSessionID, fx.agent.joinLeaderCalls[0].logSessionID)
	require.Equal(t, []election.ElectionState{
		election.ElectionStateInit,
		election.ElectionStateCanvass,
		election.ElectionStateNominate,
		election.ElectionStateCandidateBallot,
		election.ElectionStateCanvass,
		election.ElectionStateNominate,
		election.ElectionStateCandidateBallot,
		election.ElectionStateLeaderLogReplication,
		election.ElectionStateLeaderReplay,
		election.ElectionStateLeaderInit,
		election.ElectionStateLeaderReady,
	}, fx.observer.states)
}

func TestElection_shouldTimeoutFollowerBallotWithoutLeaderEmerging(t *testing.T) {
	t.Parallel()

	fx := newTimeoutElection(t, 1)

	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	candidateTermID := fx.cfg.LeadershipTermID + 1
	fx.injectRequestVote(0, candidateTermID, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition)
	require.Len(t, fx.publisher.Sent, 1)
	require.Equal(t, uint16(clusterwire.VoteSbeTemplateID), fx.publisher.Sent[0].TemplateID)

	fx.advance(1)
	fx.doWork()
	require.Equal(t, election.ElectionStateFollowerBallot, fx.election.State())

	fx.advance(fx.cfg.ElectionTimeoutNs - 2)
	fx.doWork()
	require.Equal(t, election.ElectionStateFollowerBallot, fx.election.State())

	fx.advance(2)
	fx.doWork()

	require.Equal(t, election.ElectionStateCanvass, fx.election.State())
	require.Equal(t, election.NullValue, fx.election.LeadershipTermID())
	require.Equal(t, []election.ElectionState{
		election.ElectionStateInit,
		election.ElectionStateCanvass,
		election.ElectionStateFollowerBallot,
		election.ElectionStateCanvass,
	}, fx.observer.states)
}

func TestElection_shouldBecomeFollowerIfEnteringNewElection(t *testing.T) {
	t.Parallel()

	fx := newTimeoutElection(
		t, 0,
		withNodeStartup(false),
		withPositions(1, 120, 120),
		withHostRole(consensus.RoleLeader),
	)

	fx.doWork()

	require.Equal(t, election.ElectionStateCanvass, fx.election.State())
	require.Equal(t, 1, fx.agent.prepareCalls)
	require.Equal(t, int64(120), fx.agent.prepareLogPosition)
	require.Equal(t, fx.clock.nowNs, fx.agent.prepareNowNs)
	require.Equal(t, int64(120), fx.election.AppendPosition())
	require.Equal(t, consensus.RoleFollower, fx.host.role)
	require.Equal(t, []election.ElectionState{
		election.ElectionStateInit,
		election.ElectionStateCanvass,
	}, fx.observer.states)
}

package election_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

func hostAdapterMembers(n int) []*election.Member {
	members := make([]*election.Member, n)
	for i := range n {
		members[i] = &election.Member{ID: int32(i)}
	}
	return members
}

func newTestHostAdapter(
	t *testing.T,
	members []*election.Member,
	thisIdx int,
	opts ...func(*election.HostAdapterConfig),
) (*election.HostAdapter, *consensus.Agent, *election.SbePublisher, *FakeClock) {
	t.Helper()
	require.NotEmpty(t, members)
	require.GreaterOrEqual(t, thisIdx, 0)
	require.Less(t, thisIdx, len(members))

	clock := &FakeClock{nowNs: 1_000_000_000}
	pub := &election.SbePublisher{}
	commit := new(atomic.Int64)
	agent := consensus.NewAgent(consensus.AgentConfig{
		Commit: commit,
		Role:   consensus.RoleFollower,
	})
	cfg := election.HostAdapterConfig{
		Agent:                    agent,
		Publisher:                pub,
		Members:                  members,
		ThisMember:               members[thisIdx],
		LeaderHeartbeatTimeoutNs: 10_000_000_000,
		NanoClock:                clock.TimeNs,
		LeaderMemberID:           election.NullMemberID,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	host := election.NewHostAdapter(cfg)
	agent.SetPositions(host)
	agent.SetEgress(host)
	return host, agent, pub, clock
}

func TestNewHostAdapter_panics(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	agent := consensus.NewAgent(consensus.AgentConfig{Commit: new(atomic.Int64)})
	pub := &election.SbePublisher{}
	clock := &FakeClock{nowNs: 1}

	valid := func() election.HostAdapterConfig {
		return election.HostAdapterConfig{
			Agent:                    agent,
			Publisher:                pub,
			Members:                  members,
			ThisMember:               members[0],
			LeaderHeartbeatTimeoutNs: 10,
			NanoClock:                clock.TimeNs,
			LeaderMemberID:           election.NullMemberID,
		}
	}

	tests := []struct {
		name string
		want string
		cfg  func() election.HostAdapterConfig
	}{
		{
			name: "nil Agent",
			want: "election: nil Agent",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.Agent = nil
				return c
			},
		},
		{
			name: "nil Publisher",
			want: "election: nil Publisher",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.Publisher = nil
				return c
			},
		},
		{
			name: "nil ThisMember",
			want: "election: nil ThisMember",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.ThisMember = nil
				return c
			},
		},
		{
			name: "nil NanoClock",
			want: "election: nil NanoClock",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.NanoClock = nil
				return c
			},
		},
		{
			name: "empty Members",
			want: "election: empty Members",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.Members = nil
				return c
			},
		},
		{
			name: "non-positive LeaderHeartbeatTimeoutNs",
			want: "election: non-positive LeaderHeartbeatTimeoutNs",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.LeaderHeartbeatTimeoutNs = 0
				return c
			},
		},
		{
			name: "negative LeaderHeartbeatTimeoutNs",
			want: "election: non-positive LeaderHeartbeatTimeoutNs",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.LeaderHeartbeatTimeoutNs = -1
				return c
			},
		},
		{
			name: "duplicate member id",
			want: "election: duplicate member id",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.Members = []*election.Member{
					{ID: 0},
					{ID: 1},
					{ID: 0},
				}
				c.ThisMember = c.Members[0]
				return c
			},
		},
		{
			name: "ThisMember clone same id not in slice",
			want: "election: ThisMember not in Members",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.ThisMember = &election.Member{ID: members[0].ID}
				return c
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.PanicsWithValue(t, tt.want, func() {
				_ = election.NewHostAdapter(tt.cfg())
			})
		})
	}
}

func TestNewHostAdapter_leaderMemberID_zeroIsValid(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, agent, _, _ := newTestHostAdapter(t, members, 1, func(c *election.HostAdapterConfig) {
		c.LeaderMemberID = 0
	})
	require.Equal(t, int32(0), host.LeaderMemberID())
	require.Same(t, host, agent.Positions())
	require.Same(t, host, agent.Egress())
}

func TestHostAdapter_SetLeaderMemberID(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, _, _, _ := newTestHostAdapter(t, members, 0)
	require.Equal(t, election.NullMemberID, host.LeaderMemberID())

	host.SetLeaderMemberID(2)
	require.Equal(t, int32(2), host.LeaderMemberID())

	host.SetLeaderMemberID(election.NullMemberID)
	require.Equal(t, election.NullMemberID, host.LeaderMemberID())
}

func TestHostAdapter_QuorumPositionBoundedByLeaderLog_min(t *testing.T) {
	t.Parallel()

	// nowNs=0, timeout=10, TimeOfLastAppend=0: all active (0+10 > 0). k=2.
	const (
		nowNs     int64 = 0
		timeoutNs int64 = 10
	)

	tests := []struct {
		name   string
		pos    [3]int64
		append int64
		want   int64
	}{
		// Positions 200,100,50 -> quorum 100; min(100,150)=100.
		{name: "quorum less than append", pos: [3]int64{200, 100, 50}, append: 150, want: 100},
		// Quorum 200; append 100 -> min = 100.
		{name: "quorum greater than append", pos: [3]int64{200, 200, 200}, append: 100, want: 100},
		{name: "quorum equals append", pos: [3]int64{150, 150, 0}, append: 150, want: 150},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			members := hostAdapterMembers(3)
			for i, p := range tt.pos {
				members[i].LogPosition = p
				members[i].TimeOfLastAppendPositionNs = 0
			}
			host, _, _, _ := newTestHostAdapter(t, members, 0, func(c *election.HostAdapterConfig) {
				c.LeaderHeartbeatTimeoutNs = timeoutNs
			})
			got := host.QuorumPositionBoundedByLeaderLog(tt.append, nowNs)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestHostAdapter_QuorumPositionBoundedByLeaderLog_inactiveMembers(t *testing.T) {
	t.Parallel()

	// n=3, k=2. Only member 0 active: one rank fills, quorum floor 0, min(0,200)=0.
	const (
		nowNs     int64 = 100
		timeoutNs int64 = 10
	)
	members := hostAdapterMembers(3)
	members[0].LogPosition = 300
	members[0].TimeOfLastAppendPositionNs = 95 // active: 95+10 > 100
	members[1].LogPosition = 250
	members[1].TimeOfLastAppendPositionNs = 0 // inactive: 0+10 <= 100
	members[2].LogPosition = 250
	members[2].TimeOfLastAppendPositionNs = 0

	host, _, _, _ := newTestHostAdapter(t, members, 0, func(c *election.HostAdapterConfig) {
		c.LeaderHeartbeatTimeoutNs = timeoutNs
	})
	require.Equal(t, int64(0), host.QuorumPositionBoundedByLeaderLog(200, nowNs))

	// Second member active at 250: quorum 250, min(250,200)=200.
	members[1].TimeOfLastAppendPositionNs = 95
	require.Equal(t, int64(200), host.QuorumPositionBoundedByLeaderLog(200, nowNs))
}

func TestHostAdapter_UpdateMemberLogPosition_stamps(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, _, _, clock := newTestHostAdapter(t, members, 0)
	clock.nowNs = 7_000

	host.UpdateMemberLogPosition(members[1], 9, 400)
	require.Equal(t, int64(9), members[1].LeadershipTermID)
	require.Equal(t, int64(400), members[1].LogPosition)
	require.Equal(t, int64(7_000), members[1].TimeOfLastAppendPositionNs)
}

func TestHostAdapter_UpdateMemberPosition_hit(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, _, _, clock := newTestHostAdapter(t, members, 0)
	clock.nowNs = 8_000

	host.UpdateMemberPosition(2, 11, 500)
	require.Equal(t, int64(11), members[2].LeadershipTermID)
	require.Equal(t, int64(500), members[2].LogPosition)
	require.Equal(t, int64(8_000), members[2].TimeOfLastAppendPositionNs)
}

func TestHostAdapter_UpdateMemberPosition_miss(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	before := *members[0]
	host, _, _, _ := newTestHostAdapter(t, members, 0)

	require.NotPanics(t, func() {
		host.UpdateMemberPosition(99, 1, 2)
	})
	require.Equal(t, before, *members[0])
	require.Equal(t, int64(0), members[1].LogPosition)
	require.Equal(t, int64(0), members[2].LogPosition)
}

func TestHostAdapter_UpdateMemberLogPosition_nilMember(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	before := *members[0]
	host, _, _, _ := newTestHostAdapter(t, members, 0)
	require.NotPanics(t, func() {
		host.UpdateMemberLogPosition(nil, 1, 2)
	})
	require.Equal(t, before, *members[0])
}

func TestHostAdapter_PublishCommitPosition_fanOut(t *testing.T) {
	t.Parallel()

	const (
		commit int64 = 321
		term   int64 = 7
	)

	tests := []struct {
		name    string
		thisIdx int
		wantDst map[int32]struct{}
	}{
		{
			name:    "thisIdx 0 skips self",
			thisIdx: 0,
			wantDst: map[int32]struct{}{1: {}, 2: {}},
		},
		{
			name:    "thisIdx 1 skips self",
			thisIdx: 1,
			wantDst: map[int32]struct{}{0: {}, 2: {}},
		},
		{
			name:    "thisIdx 2 skips self",
			thisIdx: 2,
			wantDst: map[int32]struct{}{0: {}, 1: {}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			members := hostAdapterMembers(3)
			host, _, pub, _ := newTestHostAdapter(t, members, tt.thisIdx)
			selfID := members[tt.thisIdx].ID

			host.PublishCommitPosition(commit, term)

			require.Len(t, pub.Sent, 2)
			dsts := make(map[int32]struct{}, 2)
			for _, msg := range pub.Sent {
				require.Equal(t, uint16(clusterwire.CommitPositionSbeTemplateID), msg.TemplateID)
				fields := msg.Fields.(codecs.CommitPositionFields)
				require.Equal(t, term, fields.LeadershipTermID)
				require.Equal(t, commit, fields.LogPosition)
				require.Equal(t, selfID, fields.LeaderMemberID)
				dsts[msg.Dst.MemberID] = struct{}{}
			}
			require.Equal(t, tt.wantDst, dsts)
			_, selfSent := dsts[selfID]
			require.False(t, selfSent)
		})
	}
}

func TestHostAdapter_PublishAppendPosition_toLeader(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, _, pub, _ := newTestHostAdapter(t, members, 1, func(c *election.HostAdapterConfig) {
		c.LeaderMemberID = 0
	})

	ok := host.PublishAppendPosition(5, 100, 3)
	require.True(t, ok)
	require.Len(t, pub.Sent, 1)
	msg := pub.Sent[0]
	require.Equal(t, uint16(clusterwire.AppendPositionSbeTemplateID), msg.TemplateID)
	require.Equal(t, int32(0), msg.Dst.MemberID)
	fields := msg.Fields.(codecs.AppendPositionFields)
	require.Equal(t, int64(5), fields.LeadershipTermID)
	require.Equal(t, int64(100), fields.LogPosition)
	require.Equal(t, int32(1), fields.FollowerMemberID)
	require.Equal(t, uint8(3), fields.Flags)
}

func TestHostAdapter_PublishAppendPosition_nullLeader(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, _, pub, _ := newTestHostAdapter(t, members, 0)
	require.False(t, host.PublishAppendPosition(1, 2, 0))
	require.Empty(t, pub.Sent)
}

func TestHostAdapter_PublishAppendPosition_leaderIsSelf(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, _, pub, _ := newTestHostAdapter(t, members, 0, func(c *election.HostAdapterConfig) {
		c.LeaderMemberID = 0
	})
	require.False(t, host.PublishAppendPosition(1, 2, 0))
	require.Empty(t, pub.Sent)
}

func TestHostAdapter_UpdateLeaderPosition_stampsThenForwards(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, agent, pub, _ := newTestHostAdapter(t, members, 0)

	// ThisMember is members[0]; stamp must mutate the ranked slice element.
	require.Equal(t, int64(0), agent.CommitPosition())
	require.Equal(t, int64(0), members[0].LogPosition)

	work := host.UpdateLeaderPosition(9_000, 200, 150)
	require.Equal(t, 1, work)
	require.Equal(t, int64(150), agent.CommitPosition())
	require.Equal(t, int64(200), members[0].LogPosition)
	require.Equal(t, int64(9_000), members[0].TimeOfLastAppendPositionNs)

	// Stamp-then-forward only; commit egress stays on PublishCommitPosition.
	require.Empty(t, pub.Sent)

	// Same quorum does not re-advance commit.
	require.Equal(t, 0, host.UpdateLeaderPosition(9_001, 200, 150))
	require.Equal(t, int64(150), agent.CommitPosition())
	require.Equal(t, int64(9_001), members[0].TimeOfLastAppendPositionNs)
	require.Empty(t, pub.Sent)
}

func TestHostAdapter_forwardSmoke(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, agent, _, _ := newTestHostAdapter(t, members, 0)

	require.Equal(t, consensus.RoleFollower, host.Role())
	host.RoleSet(consensus.RoleLeader)
	require.Equal(t, consensus.RoleLeader, host.Role())
	require.Equal(t, consensus.RoleLeader, agent.Role())

	host.SetLogRecordingID(42)
	require.Equal(t, int64(42), host.LocalLogRecordingID())
	require.Equal(t, int64(42), agent.LocalLogRecordingID())

	host.LiveLogDestinationSet("aeron:udp?endpoint=live")
	require.Equal(t, "aeron:udp?endpoint=live", host.LiveLogDestination())
	host.CatchupLogDestinationSet("aeron:udp?endpoint=catchup")
	require.Equal(t, "aeron:udp?endpoint=catchup", host.CatchupLogDestination())

	require.Equal(t, int64(0), host.TimeOfLastLeaderUpdateNs())
	require.Equal(t, 0, host.CatchupPoll(0, 0))
	require.False(t, host.IsCatchupNearLive(0))
	require.Equal(t, int32(election.NullMemberID), host.AddLogPublication(100))
	require.False(t, host.AppendNewLeadershipTermEvent(1))
	require.Equal(t, 0, host.PollArchiveEvents())
	require.NotPanics(t, func() {
		host.CatchupInitiated(1)
		host.StopAllCatchups()
		host.TruncateLogEntry(1, 2)
		host.AwaitLocalSocketsClosed(3)
		host.TrackCatchupCompletion(members[1], 1, 0)
	})
	params, ok := host.FollowerLogJoinReady(1)
	require.False(t, ok)
	require.Equal(t, consensus.FollowerLogJoinParams{}, params)
	require.Nil(t, host.NewLogReplication("a", "b", 1, 2, 3))

	replay := host.NewLogReplay(10, 200)
	require.NotNil(t, replay)
	require.True(t, replay.IsDone())
	require.Equal(t, int64(200), replay.Position())
	require.Equal(t, 0, replay.DoWork(0))
}

// TestHostAdapter_leaderLogReplicationCommitFanOut proves live ranking and
// CommitPosition fan-out through HostAdapter as ElectionHost under
// LEADER_LOG_REPLICATION with shared SbePublisher egress.
func TestHostAdapter_leaderLogReplicationCommitFanOut(t *testing.T) {
	t.Parallel()

	// Keep follower log below append so first tick stays in LEADER_LOG_REPLICATION.
	const followerLogPos int64 = 50

	members := hostAdapterMembers(3)
	cfg, deps, _, _, _ := electionDepsForMemberPtrs(t, members, 0)
	cfg.IsNodeStartup = true
	cfg.LeadershipTermID = testLeadershipTermID
	cfg.LogPosition = testLogPosition
	cfg.AppendPosition = testAppendPosition
	cfg.MemberID = 0

	host, agent, pub, clock := newTestHostAdapter(t, members, 0, func(c *election.HostAdapterConfig) {
		c.LeaderHeartbeatTimeoutNs = cfg.LeaderHeartbeatTimeoutNs
	})
	epochClock := &FakeEpochClock{nowMs: 42_000}

	deps.Host = host
	deps.Handler = agent
	deps.Publisher = pub
	deps.NanoClock = clock.TimeNs
	deps.EpochClock = epochClock.TimeMs
	deps.NominationRand = func() float64 { return 0 }
	deps.Members = members
	deps.ThisMember = members[0]

	e := election.NewElection(cfg, deps)
	agent.SetElection(e)

	// Followers stamped below append so live quorum stays under the leader log bound.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	injectCanvassPosition(e, 1, cfg.LeadershipTermID, cfg.AppendPosition)
	injectCanvassPosition(e, 2, cfg.LeadershipTermID, cfg.AppendPosition)

	// nominationRand=0 nominates immediately; advance until candidate ballot.
	for range 30 {
		if e.State() == election.ElectionStateCandidateBallot {
			break
		}
		e.DoWork(clock.nowNs)
		clock.nowNs += cfg.ElectionStatusIntervalNs
		epochClock.nowMs += cfg.ElectionStatusIntervalNs / 1_000_000
	}
	require.Equal(t, election.ElectionStateCandidateBallot, e.State())

	yes := clusterwire.BooleanType_TRUE
	e.OnVote(codecs.VoteFields{
		CandidateTermID:     election.CandidateTermIDForTest(e),
		LogLeadershipTermID: cfg.LeadershipTermID,
		LogPosition:         followerLogPos,
		CandidateMemberID:   e.ThisMemberID(),
		FollowerMemberID:    1,
		Vote:                yes,
	})
	e.OnVote(codecs.VoteFields{
		CandidateTermID:     election.CandidateTermIDForTest(e),
		LogLeadershipTermID: cfg.LeadershipTermID,
		LogPosition:         followerLogPos,
		CandidateMemberID:   e.ThisMemberID(),
		FollowerMemberID:    2,
		Vote:                yes,
	})
	require.Equal(t, followerLogPos, members[1].LogPosition)
	require.Equal(t, followerLogPos, members[2].LogPosition)
	require.Equal(t, cfg.LeadershipTermID, members[1].LeadershipTermID)
	require.Equal(t, cfg.LeadershipTermID, members[2].LeadershipTermID)
	require.Equal(t, clock.nowNs, members[1].TimeOfLastAppendPositionNs)
	require.Equal(t, clock.nowNs, members[2].TimeOfLastAppendPositionNs)

	e.DoWork(clock.nowNs)
	require.Equal(t, election.ElectionStateLeaderLogReplication, e.State())
	require.Equal(t, consensus.RoleLeader, host.Role())

	// NLT encode needs a real log session; commit fan-out does not.
	election.SetLogSessionIDForTest(e, fixtureLogSessionID)
	pub.Reset()

	// Quorum min(rank, append) should fan out CommitPosition at followerLogPos.
	work := e.DoWork(clock.nowNs)
	require.Positive(t, work)
	require.Equal(t, election.ElectionStateLeaderLogReplication, e.State())
	require.Equal(t, cfg.AppendPosition, members[0].LogPosition)
	require.Equal(t, clock.nowNs, members[0].TimeOfLastAppendPositionNs)

	var commits []election.SentMessage
	for _, msg := range pub.Sent {
		if msg.TemplateID == clusterwire.CommitPositionSbeTemplateID {
			commits = append(commits, msg)
		}
	}
	require.Len(t, commits, 2, "commit fan-out must reach both peers")
	term := e.LeadershipTermID()
	dsts := make(map[int32]struct{}, 2)
	for _, msg := range commits {
		fields := msg.Fields.(codecs.CommitPositionFields)
		require.Equal(t, term, fields.LeadershipTermID)
		require.Equal(t, followerLogPos, fields.LogPosition)
		require.Equal(t, int32(0), fields.LeaderMemberID)
		dsts[msg.Dst.MemberID] = struct{}{}
	}
	require.Equal(t, map[int32]struct{}{1: {}, 2: {}}, dsts)
}

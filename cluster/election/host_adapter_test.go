package election_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
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
		Commit:         commit,
		Role:           consensus.RoleFollower,
		ThisMemberID:   members[thisIdx].ID,
		LeaderMemberID: election.NullMemberID,
	})
	cfg := election.HostAdapterConfig{
		Agent:                    agent,
		Publisher:                pub,
		Members:                  members,
		ThisMember:               members[thisIdx],
		LeaderHeartbeatTimeoutNs: 10_000_000_000,
		NanoClock:                clock.TimeNs,
		EpochClock:               func() int64 { return 42_000 },
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

// newHostAdapterWithElection wires a real HostAdapter as ElectionHost over
// baseline testElectionDeps members (thisIdx 0).
func newHostAdapterWithElection(t *testing.T) (
	cfg election.ElectionConfig,
	host *election.HostAdapter,
	agent *consensus.Agent,
	e *election.Election,
) {
	t.Helper()
	cfg, deps, _, _, _ := testElectionDeps(t)
	host, agent, _, _ = newTestHostAdapter(t, deps.Members, 0)
	deps.Host = host
	deps.Publisher = &election.SbePublisher{}
	e = election.NewElection(cfg, deps)
	return cfg, host, agent, e
}

// requireCommitPositionFanOut asserts CommitPosition messages in sent match
// term/logPos/leaderID and exactly the wantDst member set (skip-self peers).
func requireCommitPositionFanOut(
	t *testing.T,
	sent []election.SentMessage,
	term, logPos int64,
	leaderID int32,
	wantDst map[int32]struct{},
) {
	t.Helper()
	commits := make([]election.SentMessage, 0, len(wantDst))
	for _, msg := range sent {
		if msg.TemplateID == clusterwire.CommitPositionSbeTemplateID {
			commits = append(commits, msg)
		}
	}
	require.Len(t, commits, len(wantDst))
	dsts := make(map[int32]struct{}, len(wantDst))
	for _, msg := range commits {
		fields := msg.Fields.(codecs.CommitPositionFields)
		require.Equal(t, term, fields.LeadershipTermID)
		require.Equal(t, logPos, fields.LogPosition)
		require.Equal(t, leaderID, fields.LeaderMemberID)
		dsts[msg.Dst.MemberID] = struct{}{}
	}
	require.Equal(t, wantDst, dsts)
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
			EpochClock:               func() int64 { return 1 },
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
			name: "nil EpochClock",
			want: "election: nil EpochClock",
			cfg: func() election.HostAdapterConfig {
				c := valid()
				c.EpochClock = nil
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

	// Construct seeds Agent via SetLeaderMemberID (single store).
	require.Equal(t, int32(0), agent.LeaderMemberID())
	require.Same(t, host, agent.Positions())
	require.Same(t, host, agent.Egress())
}

// Construct seeds Agent.ThisMemberID from ThisMember even when AgentConfig
// left the default zero (zero is a valid member id elsewhere).
func TestNewHostAdapter_seedsThisMemberID(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	const thisIdx = 2
	clock := &FakeClock{nowNs: 1_000_000_000}
	pub := &election.SbePublisher{}

	// AgentConfig omits ThisMemberID (default 0). Host ThisMember is id 2.
	agent := consensus.NewAgent(consensus.AgentConfig{
		Commit:         new(atomic.Int64),
		Role:           consensus.RoleLeader,
		LeaderMemberID: election.NullMemberID,
	})
	require.Equal(t, int32(0), agent.ThisMemberID())

	host := election.NewHostAdapter(election.HostAdapterConfig{
		Agent:                    agent,
		Publisher:                pub,
		Members:                  members,
		ThisMember:               members[thisIdx],
		LeaderHeartbeatTimeoutNs: 10_000_000_000,
		NanoClock:                clock.TimeNs,
		EpochClock:               func() int64 { return 42_000 },
		LeaderMemberID:           election.NullMemberID,
	})
	agent.SetPositions(host)
	agent.SetEgress(host)

	require.Equal(t, members[thisIdx].ID, agent.ThisMemberID())
	require.Equal(t, int32(2), agent.ThisMemberID())

	// Steady leader stamp uses the seeded id, not default 0.
	appendV := new(atomic.Int64)
	appendV.Store(100)
	agent.SetAppendSource(&consensus.AtomicAppendPosition{V: appendV})

	// Pre-seed peer activity so quorum can form; stamp still records thisMember.
	host.UpdateMemberPosition(0, 1, 100)
	host.UpdateMemberPosition(1, 1, 100)
	agent.SetLeadershipTermID(1)
	require.Equal(t, 1, agent.DoWork(clock.TimeNs()))
	require.Equal(t, int64(100), members[thisIdx].LogPosition)
	require.Equal(t, int64(1), members[thisIdx].LeadershipTermID)

	// Member 0 must not have been overwritten by a mistaken self-stamp of id 0
	// after the pre-seed (self is 2).
	require.Equal(t, int64(100), members[0].LogPosition)
}

func TestHostAdapter_SetLeaderMemberID(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, agent, _, _ := newTestHostAdapter(t, members, 0)
	require.Equal(t, election.NullMemberID, host.LeaderMemberID())
	require.Equal(t, election.NullMemberID, agent.LeaderMemberID())

	host.SetLeaderMemberID(2)
	require.Equal(t, int32(2), host.LeaderMemberID())
	require.Equal(t, int32(2), agent.LeaderMemberID())

	// Member id 0 is valid; must not rewrite to NullMemberID.
	host.SetLeaderMemberID(0)
	require.Equal(t, int32(0), host.LeaderMemberID())
	require.Equal(t, int32(0), agent.LeaderMemberID())

	host.SetLeaderMemberID(election.NullMemberID)
	require.Equal(t, election.NullMemberID, host.LeaderMemberID())
	require.Equal(t, election.NullMemberID, agent.LeaderMemberID())

	// Agent and Host read the same Agent store.
	agent.SetLeaderMemberID(1)
	require.Equal(t, int32(1), host.LeaderMemberID())
	require.Equal(t, int32(1), agent.LeaderMemberID())
}

func TestHostAdapter_ApplyLeadershipTerm(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, agent, _, _ := newTestHostAdapter(t, members, 0)
	require.Equal(t, int64(0), agent.LeadershipTermID())

	host.ApplyLeadershipTerm(7)
	require.Equal(t, int64(7), agent.LeadershipTermID())

	host.ApplyLeadershipTerm(0)
	require.Equal(t, int64(0), agent.LeadershipTermID())
}

// TestInstallLeadershipTerm_handoffAgentLeaderAndTerm drives follower
// NewLeadershipTerm install through HostAdapter + real Agent and asserts
// leader id and Agent leadership term handoff. Truncating path must not hand
// off. After complete, same-term steady Commit advances NotifiedCommitPosition.
func TestInstallLeadershipTerm_handoffAgentLeaderAndTerm(t *testing.T) {
	t.Parallel()

	const leaderID int32 = 1

	t.Run("adopt sets leader and term on Agent", func(t *testing.T) {
		t.Parallel()

		cfg, host, agent, e := newHostAdapterWithElection(t)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		require.Equal(t, election.NullMemberID, host.LeaderMemberID())
		require.Equal(t, election.NullMemberID, agent.LeaderMemberID())
		require.Equal(t, int64(0), agent.LeadershipTermID())

		fields := installNewLeadershipTermFields(cfg, leaderID, cfg.LeadershipTermID)
		e.OnNewLeadershipTerm(fields)

		require.Equal(t, election.ElectionStateFollowerReplay, e.State())
		require.Equal(t, leaderID, host.LeaderMemberID())
		require.Equal(t, leaderID, agent.LeaderMemberID())
		require.Equal(t, fields.LeadershipTermID, agent.LeadershipTermID())
		require.Equal(t, fields.LeadershipTermID, e.LeadershipTermID())
	})

	t.Run("after complete steady commit notifies", func(t *testing.T) {
		t.Parallel()

		cfg, _, agent, e := newHostAdapterWithElection(t)

		// Wire election onto Agent so ElectionComplete clears a live runner.
		agent.SetElection(e)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		fields := installNewLeadershipTermFields(cfg, leaderID, cfg.LeadershipTermID)
		e.OnNewLeadershipTerm(fields)
		require.Equal(t, leaderID, agent.LeaderMemberID())
		require.Equal(t, fields.LeadershipTermID, agent.LeadershipTermID())
		require.Equal(t, consensus.RoleFollower, agent.Role())
		require.Equal(t, int64(0), agent.NotifiedCommitPosition())

		const completeNs int64 = 2_000
		agent.ElectionComplete(completeNs)
		require.Nil(t, agent.Election())

		// Handoff fields survive complete.
		require.Equal(t, leaderID, agent.LeaderMemberID())
		require.Equal(t, fields.LeadershipTermID, agent.LeadershipTermID())

		const commitPos int64 = 777
		const pollNs int64 = 3_000
		q := &consensus.QueueConsensusIngress{}
		agent.SetIngress(q)
		q.Enqueue(encodeCommitPositionWireForTest(t, codecs.CommitPositionFields{
			LeadershipTermID: fields.LeadershipTermID,
			LogPosition:      commitPos,
			LeaderMemberID:   leaderID,
		}))

		// DoWork: pollIngress (steady) then consensusWork stub.
		work := agent.DoWork(pollNs)
		require.Equal(t, 1, work)
		require.Equal(t, commitPos, agent.NotifiedCommitPosition())
		require.Equal(t, pollNs, agent.TimeOfLastLeaderUpdateNs())
	})

	t.Run("truncate skips handoff", func(t *testing.T) {
		t.Parallel()

		cfg, host, agent, e := newHostAdapterWithElection(t)

		// Pre-seed a non-null leader/term so a mistaken handoff is visible.
		const seedLeader int32 = 2
		const seedTerm int64 = 3
		host.SetLeaderMemberID(seedLeader)
		host.ApplyLeadershipTerm(seedTerm)
		require.Equal(t, seedLeader, agent.LeaderMemberID())
		require.Equal(t, seedTerm, agent.LeadershipTermID())

		election.SetStateForTest(e, election.ElectionStateCanvass)

		fields := installNewLeadershipTermFields(cfg, leaderID, cfg.LeadershipTermID)
		fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
		fields.NextTermBaseLogPosition = cfg.AppendPosition - 50
		e.OnNewLeadershipTerm(fields)

		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, cfg.AppendPosition-50, e.AppendPosition())
		require.Equal(t, seedLeader, host.LeaderMemberID())
		require.Equal(t, seedLeader, agent.LeaderMemberID())
		require.Equal(t, seedTerm, agent.LeadershipTermID())
	})

	// Lagging append with null successor term: handoff runs on adopt, then
	// HandleError returns to Init. Agent/host stay on adopted leader+term.
	t.Run("invalid NewLeadershipTerm after handoff keeps adopted leader and term", func(t *testing.T) {
		t.Parallel()

		cfg, host, agent, e := newHostAdapterWithElection(t)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		fields := installNewLeadershipTermFields(cfg, leaderID, cfg.LeadershipTermID)
		fields.TermBaseLogPosition = cfg.AppendPosition + 100
		e.OnNewLeadershipTerm(fields)

		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, leaderID, host.LeaderMemberID())
		require.Equal(t, leaderID, agent.LeaderMemberID())
		require.Equal(t, fields.LeadershipTermID, agent.LeadershipTermID())
		require.Equal(t, fields.LeadershipTermID, e.LeadershipTermID())
	})
}

// encodeCommitPositionWireForTest builds a full CommitPosition SBE frame via
// [election.SbePublisher] for steady Agent ingress inject.
func encodeCommitPositionWireForTest(t *testing.T, fields codecs.CommitPositionFields) []byte {
	t.Helper()
	pub := &election.SbePublisher{}
	pub.CommitPosition(election.PublicationTarget{MemberID: 0}, fields)
	require.Len(t, pub.Sent, 1)
	return pub.Sent[0].Wire
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
	}{
		{name: "thisIdx 0 skips self", thisIdx: 0},
		{name: "thisIdx 1 skips self", thisIdx: 1},
		{name: "thisIdx 2 skips self", thisIdx: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			members := hostAdapterMembers(3)
			host, _, pub, _ := newTestHostAdapter(t, members, tt.thisIdx)
			selfID := members[tt.thisIdx].ID

			wantDst := make(map[int32]struct{}, len(members)-1)
			for _, m := range members {
				if m.ID != selfID {
					wantDst[m.ID] = struct{}{}
				}
			}

			host.PublishCommitPosition(commit, term)
			require.Len(t, pub.Sent, len(wantDst))
			requireCommitPositionFanOut(t, pub.Sent, term, commit, selfID, wantDst)
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

func TestHostAdapter_PublishAppendPosition_noTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		leaderID int32
	}{
		{name: "null leader", leaderID: election.NullMemberID},
		{name: "leader is self", leaderID: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			members := hostAdapterMembers(3)
			host, _, pub, _ := newTestHostAdapter(t, members, 0, func(c *election.HostAdapterConfig) {
				c.LeaderMemberID = tt.leaderID
			})
			require.False(t, host.PublishAppendPosition(1, 2, 0))
			require.Empty(t, pub.Sent)
		})
	}
}

func TestHostAdapter_UpdateLeaderPosition_stampsThenForwards(t *testing.T) {
	t.Parallel()

	const (
		term   int64 = 11
		quorum int64 = 150
	)

	members := hostAdapterMembers(3)
	host, agent, pub, _ := newTestHostAdapter(t, members, 0)
	agent.SetLeadershipTermID(term)
	selfID := members[0].ID

	// ThisMember is members[0]; stamp must mutate the ranked slice element.
	require.Equal(t, int64(0), agent.CommitPosition())
	require.Equal(t, int64(0), members[0].LogPosition)

	work := host.UpdateLeaderPosition(9_000, 200, quorum)
	require.Equal(t, 1, work)
	require.Equal(t, quorum, agent.CommitPosition())
	require.Equal(t, int64(200), members[0].LogPosition)
	require.Equal(t, int64(9_000), members[0].TimeOfLastAppendPositionNs)

	// HostAdapter.UpdateLeaderPosition only stamps then forwards; Agent gate
	// publishes CommitPosition via the egress surface (same HostAdapter).
	// Skip-self fan-out to the other two members.
	wantDst := map[int32]struct{}{1: {}, 2: {}}
	require.Len(t, pub.Sent, len(wantDst))
	requireCommitPositionFanOut(t, pub.Sent, term, quorum, selfID, wantDst)
	firstSent := len(pub.Sent)

	// Same quorum inside heartbeat interval: no re-publish, stamp still runs.
	require.Equal(t, 0, host.UpdateLeaderPosition(9_001, 200, quorum))
	require.Equal(t, quorum, agent.CommitPosition())
	require.Equal(t, int64(9_001), members[0].TimeOfLastAppendPositionNs)
	require.Len(t, pub.Sent, firstSent)
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

// HostAdapter TrackCatchupCompletion is a no-op (does not panic).
//
// OnAppendPosition still invokes TrackCatchup on the election host.
func TestTrackCatchupCompletion_hostAdapterNoOp(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, _, _, _ := newTestHostAdapter(t, members, 0)

	require.NotPanics(t, func() {
		host.TrackCatchupCompletion(members[1], 1, 0x03)
		host.TrackCatchupCompletion(nil, 0, 0)
	})

	// OnAppendPosition still reaches TrackCatchupCompletion via ElectionHost.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	recording := &recordingElectionHost{clock: clock, role: consensus.RoleFollower}
	deps.Host = recording
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	const flags uint8 = 0x04
	e.OnAppendPosition(codecs.AppendPositionFields{
		LeadershipTermID: testLeadershipTermID,
		LogPosition:      100,
		FollowerMemberID: 1,
		Flags:            flags,
	})
	require.Len(t, recording.trackCatchupCalls, 1)
	require.Equal(t, int32(1), recording.trackCatchupCalls[0].memberID)
	require.Equal(t, int64(testLeadershipTermID), recording.trackCatchupCalls[0].leadershipTermID)
	require.Equal(t, flags, recording.trackCatchupCalls[0].flags)
}

// TestHostAdapter_leaderLogReplicationCommitFanOut proves live ranking and
// CommitPosition fan-out through HostAdapter as ElectionHost under
// LEADER_LOG_REPLICATION with shared SbePublisher egress.
func TestHostAdapter_leaderLogReplicationCommitFanOut(t *testing.T) {
	t.Parallel()

	// Keep follower log below append so first tick stays in LEADER_LOG_REPLICATION.
	const followerLogPos int64 = 50

	members := hostAdapterMembers(3)
	cfg, deps, _, epochClock, _ := electionDepsForMemberPtrs(t, members, 0)
	cfg.IsNodeStartup = true
	cfg.LeadershipTermID = testLeadershipTermID
	cfg.LogPosition = testLogPosition
	cfg.AppendPosition = testAppendPosition
	cfg.MemberID = 0

	host, agent, pub, clock := newTestHostAdapter(t, members, 0, func(c *election.HostAdapterConfig) {
		c.LeaderHeartbeatTimeoutNs = cfg.LeaderHeartbeatTimeoutNs
	})

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
		advanceFakeClocks(clock, epochClock, cfg.ElectionStatusIntervalNs)
	}
	require.Equal(t, election.ElectionStateCandidateBallot, e.State())

	for _, followerID := range []int32{1, 2} {
		e.OnVote(codecs.VoteFields{
			CandidateTermID:     election.CandidateTermIDForTest(e),
			LogLeadershipTermID: cfg.LeadershipTermID,
			LogPosition:         followerLogPos,
			CandidateMemberID:   e.ThisMemberID(),
			FollowerMemberID:    followerID,
			Vote:                clusterwire.BooleanType_TRUE,
		})
	}
	for _, followerID := range []int32{1, 2} {
		m := members[followerID]
		require.Equal(t, followerLogPos, m.LogPosition)
		require.Equal(t, cfg.LeadershipTermID, m.LeadershipTermID)
		require.Equal(t, clock.nowNs, m.TimeOfLastAppendPositionNs)
	}

	e.DoWork(clock.nowNs)
	require.Equal(t, election.ElectionStateLeaderLogReplication, e.State())
	require.Equal(t, consensus.RoleLeader, host.Role())

	// NewLeadershipTerm encode needs a real log session; commit fan-out does not.
	election.SetLogSessionIDForTest(e, fixtureLogSessionID)
	pub.Reset()

	// Quorum min(rank, append) should fan out CommitPosition at followerLogPos.
	work := e.DoWork(clock.nowNs)
	require.Positive(t, work)
	require.Equal(t, election.ElectionStateLeaderLogReplication, e.State())
	require.Equal(t, cfg.AppendPosition, members[0].LogPosition)
	require.Equal(t, clock.nowNs, members[0].TimeOfLastAppendPositionNs)

	requireCommitPositionFanOut(
		t, pub.Sent, e.LeadershipTermID(), followerLogPos, 0,
		map[int32]struct{}{1: {}, 2: {}},
	)
}

// mapTermLookup is a RecordingLogLookup for PublishNewLeadershipTerm tests.
type mapTermLookup map[int64]*recordinglog.Entry

func (m mapTermLookup) FindTermEntry(logLeadershipTermID int64) *recordinglog.Entry {
	return m[logLeadershipTermID]
}

// TestHostAdapter_PublishNewLeadershipTerm_answerConstruction asserts the ten
// settled-answer construction rows against the real adapter method.
func TestHostAdapter_PublishNewLeadershipTerm_answerConstruction(t *testing.T) {
	t.Parallel()

	const (
		thisIdx     = 0
		followerID  = int32(1)
		leaderTerm  = int64(5)
		termBase    = int64(200)
		logPos      = int64(400)
		commitPos   = int64(350)
		recordingID = int64(88)
		sessionID   = int32(42)
		appVersion  = int32(-7)
		epochStamp  = int64(99_000)
	)

	baseAnswer := consensus.CanvassAnswer{
		FollowerMemberID:    followerID,
		LogLeadershipTermID: leaderTerm,
		LeadershipTermID:    leaderTerm,
		LogPosition:         logPos,
		CommitPosition:      commitPos,
		LeaderRecordingID:   recordingID,
		LogSessionID:        sessionID,
	}

	currentEntry := &recordinglog.Entry{
		LeadershipTermID:    leaderTerm,
		TermBaseLogPosition: termBase,
		LogPosition:         logPos,
	}

	newAdapter := func(
		t *testing.T,
		lookup election.RecordingLogLookup,
	) (*election.HostAdapter, *election.SbePublisher) {
		t.Helper()
		members := hostAdapterMembers(3)
		host, _, pub, _ := newTestHostAdapter(t, members, thisIdx, func(c *election.HostAdapterConfig) {
			c.EpochClock = func() int64 { return epochStamp }
			c.RecordingLogLookup = lookup
			c.AppVersion = appVersion
		})
		return host, pub
	}

	t.Run("current term canvasser null next fields", func(t *testing.T) {
		t.Parallel()
		host, pub := newAdapter(t, mapTermLookup{leaderTerm: currentEntry})
		ok := host.PublishNewLeadershipTerm(baseAnswer)
		require.True(t, ok)
		require.Len(t, pub.Sent, 1)
		require.Equal(t, followerID, pub.Sent[0].Dst.MemberID)
		fields := pub.Sent[0].Fields.(codecs.NewLeadershipTermFields)
		require.Equal(t, election.NullValue, fields.NextLeadershipTermID)
		require.Equal(t, election.NullPosition, fields.NextTermBaseLogPosition)
		require.Equal(t, election.NullPosition, fields.NextLogPosition)
		require.Equal(t, termBase, fields.TermBaseLogPosition)
		require.Equal(t, leaderTerm, fields.LogLeadershipTermID)
		require.Equal(t, leaderTerm, fields.LeadershipTermID)
	})

	t.Run("lagging canvasser next term indexed", func(t *testing.T) {
		t.Parallel()
		// One term behind: FindTermEntry(canvass+1) is the current term entry.
		lookup := mapTermLookup{leaderTerm: currentEntry}
		answer := baseAnswer
		answer.LogLeadershipTermID = leaderTerm - 1
		host, pub := newAdapter(t, lookup)
		ok := host.PublishNewLeadershipTerm(answer)
		require.True(t, ok)
		require.Len(t, pub.Sent, 1)
		fields := pub.Sent[0].Fields.(codecs.NewLeadershipTermFields)
		require.Equal(t, answer.LogLeadershipTermID, fields.LogLeadershipTermID)
		require.Equal(t, answer.LeadershipTermID, fields.LeadershipTermID)
		require.Equal(t, termBase, fields.TermBaseLogPosition)
		require.Equal(t, currentEntry.LeadershipTermID, fields.NextLeadershipTermID)
		require.Equal(t, currentEntry.TermBaseLogPosition, fields.NextTermBaseLogPosition)
		require.Equal(t, currentEntry.LogPosition, fields.NextLogPosition)
	})

	t.Run("lagging canvasser next term not indexed", func(t *testing.T) {
		t.Parallel()
		// Canvasser at 3, next key 4 missing: fallback to leader term and base.
		lookup := mapTermLookup{leaderTerm: currentEntry}
		answer := baseAnswer
		answer.LogLeadershipTermID = 3
		host, pub := newAdapter(t, lookup)
		ok := host.PublishNewLeadershipTerm(answer)
		require.True(t, ok)
		require.Len(t, pub.Sent, 1)
		fields := pub.Sent[0].Fields.(codecs.NewLeadershipTermFields)
		require.Equal(t, answer.LogLeadershipTermID, fields.LogLeadershipTermID)
		require.Equal(t, answer.LeadershipTermID, fields.LeadershipTermID)
		require.Equal(t, termBase, fields.TermBaseLogPosition)
		require.Equal(t, leaderTerm, fields.NextLeadershipTermID)
		require.Equal(t, termBase, fields.NextTermBaseLogPosition)
		require.Equal(t, election.NullPosition, fields.NextLogPosition)
	})

	t.Run("lagging next term uncommitted keeps null log position", func(t *testing.T) {
		t.Parallel()
		// Distinct next entry with null log position; no append sub.
		// Leader term 10, canvasser at 5, next at 6 uncommitted; current at 10.
		const (
			highTerm       int64 = 10
			canvassTerm    int64 = 5
			nextKey        int64 = 6
			highBase       int64 = 1000
			nextBase       int64 = 700
			nextLeadership int64 = 6
		)
		lookup := mapTermLookup{
			highTerm: {
				LeadershipTermID:    highTerm,
				TermBaseLogPosition: highBase,
				LogPosition:         logPos,
			},
			nextKey: {
				LeadershipTermID:    nextLeadership,
				TermBaseLogPosition: nextBase,
				LogPosition:         recordinglog.NullPosition,
			},
		}
		answer := baseAnswer
		answer.LeadershipTermID = highTerm
		answer.LogLeadershipTermID = canvassTerm
		host, pub := newAdapter(t, lookup)
		ok := host.PublishNewLeadershipTerm(answer)
		require.True(t, ok)
		require.Len(t, pub.Sent, 1)
		fields := pub.Sent[0].Fields.(codecs.NewLeadershipTermFields)
		require.Equal(t, answer.LogLeadershipTermID, fields.LogLeadershipTermID)
		require.Equal(t, answer.LeadershipTermID, fields.LeadershipTermID)
		require.Equal(t, highBase, fields.TermBaseLogPosition)
		require.Equal(t, nextLeadership, fields.NextLeadershipTermID)
		require.Equal(t, nextBase, fields.NextTermBaseLogPosition)
		require.Equal(t, election.NullPosition, fields.NextLogPosition)
	})

	t.Run("current term not indexed refuses", func(t *testing.T) {
		t.Parallel()
		host, pub := newAdapter(t, mapTermLookup{})
		ok := host.PublishNewLeadershipTerm(baseAnswer)
		require.False(t, ok)
		require.Empty(t, pub.Sent)
	})

	t.Run("nil recording log lookup refuses", func(t *testing.T) {
		t.Parallel()
		host, pub := newAdapter(t, nil)
		ok := host.PublishNewLeadershipTerm(baseAnswer)
		require.False(t, ok)
		require.Empty(t, pub.Sent)
	})

	t.Run("unknown member refuses", func(t *testing.T) {
		t.Parallel()
		host, pub := newAdapter(t, mapTermLookup{leaderTerm: currentEntry})
		answer := baseAnswer
		answer.FollowerMemberID = 99
		ok := host.PublishNewLeadershipTerm(answer)
		require.False(t, ok)
		require.Empty(t, pub.Sent)
	})

	t.Run("self member refuses", func(t *testing.T) {
		t.Parallel()
		host, pub := newAdapter(t, mapTermLookup{leaderTerm: currentEntry})
		answer := baseAnswer
		answer.FollowerMemberID = int32(thisIdx)
		ok := host.PublishNewLeadershipTerm(answer)
		require.False(t, ok)
		require.Empty(t, pub.Sent)
	})

	t.Run("null log session refuses", func(t *testing.T) {
		t.Parallel()
		host, pub := newAdapter(t, mapTermLookup{leaderTerm: currentEntry})
		answer := baseAnswer
		answer.LogSessionID = election.NullMemberID
		ok := host.PublishNewLeadershipTerm(answer)
		require.False(t, ok)
		require.Empty(t, pub.Sent)
	})

	t.Run("published answer stamps identity clock and version", func(t *testing.T) {
		t.Parallel()
		host, pub := newAdapter(t, mapTermLookup{leaderTerm: currentEntry})
		ok := host.PublishNewLeadershipTerm(baseAnswer)
		require.True(t, ok)
		require.Len(t, pub.Sent, 1)
		fields := pub.Sent[0].Fields.(codecs.NewLeadershipTermFields)
		require.Equal(t, int32(thisIdx), fields.LeaderMemberID)
		require.Equal(t, clusterwire.BooleanType_FALSE, fields.IsStartup)
		require.Equal(t, epochStamp, fields.Timestamp)
		require.Equal(t, appVersion, fields.AppVersion)
		require.Equal(t, logPos, fields.LogPosition)
		require.Equal(t, commitPos, fields.CommitPosition)
		require.Equal(t, recordingID, fields.LeaderRecordingID)
		require.Equal(t, sessionID, fields.LogSessionID)
	})
}

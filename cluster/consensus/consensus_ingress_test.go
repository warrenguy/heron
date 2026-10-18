package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

type recordingRunner struct {
	handles [][]byte
}

func (r *recordingRunner) DoWork(int64) int { return 0 }

func (r *recordingRunner) HandleConsensusMessage(buf []byte, offset int) bool {
	cp := make([]byte, len(buf)-offset)
	copy(cp, buf[offset:])
	r.handles = append(r.handles, cp)
	return true
}

type recordingPositionTable struct {
	updates  []memberPositionUpdate
	liveness []int32
}

type memberPositionUpdate struct {
	memberID         int32
	leadershipTermID int64
	logPosition      int64
}

func (p *recordingPositionTable) UpdateMemberPosition(
	memberID int32,
	leadershipTermID int64,
	logPosition int64,
) {
	p.updates = append(p.updates, memberPositionUpdate{
		memberID:         memberID,
		leadershipTermID: leadershipTermID,
		logPosition:      logPosition,
	})
}

func (p *recordingPositionTable) RefreshMemberLiveness(memberID int32) {
	p.liveness = append(p.liveness, memberID)
}

func (p *recordingPositionTable) QuorumPositionBoundedByLeaderLog(
	appendPosition, nowNs int64,
) int64 {
	return appendPosition
}

func (p *recordingPositionTable) HasActiveQuorum(nowNs, timeoutNs int64) bool {
	return true
}

func encodeAppendPositionWire(t *testing.T, fields codecs.AppendPositionFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.AppendPositionSbeSchemaVersion)
	blockLen := codecs.AppendPositionBlockLength(actingVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(blockLen)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeAppendPosition(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func encodeCommitPositionWire(t *testing.T, fields codecs.CommitPositionFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.CommitPositionSbeSchemaVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(clusterwire.CommitPositionSbeBlockLength)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeCommitPosition(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func encodeNewLeadershipTermWire(t *testing.T, fields codecs.NewLeadershipTermFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.NewLeadershipTermSbeSchemaVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(
		codecs.NewLeadershipTermBlockLength(actingVersion),
	)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeNewLeadershipTerm(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func encodeCanvassPositionWire(t *testing.T, fields codecs.CanvassPositionFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.CanvassPositionSbeSchemaVersion)
	blockLen := codecs.CanvassPositionBlockLength(actingVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(blockLen)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeCanvassPosition(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func encodeRequestVoteWire(t *testing.T, fields codecs.RequestVoteFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.RequestVoteSbeSchemaVersion)
	blockLen := codecs.RequestVoteBlockLength(actingVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(blockLen)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeRequestVote(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func followerWithIngress(
	leadershipTermID int64,
	leaderMemberID int32,
	notifiedCommit int64,
) (*Agent, *QueueConsensusIngress) {
	q := &QueueConsensusIngress{}
	a := NewAgent(AgentConfig{
		Role:             RoleFollower,
		Ingress:          q,
		LeadershipTermID: leadershipTermID,
		LeaderMemberID:   leaderMemberID,
	})
	a.notifiedCommitPosition = notifiedCommit
	return a, q
}

func requireSteadyPoll(
	t *testing.T,
	a *Agent,
	nowNs int64,
	wantNotify int64,
	wantLeaderUpdateNs int64,
	wantLogUpdateNs int64,
) {
	t.Helper()
	require.Equal(t, 1, a.pollConsensusIngress(nowNs))
	require.Equal(t, wantNotify, a.NotifiedCommitPosition())
	require.Equal(t, wantLeaderUpdateNs, a.TimeOfLastLeaderUpdateNs())
	require.Equal(t, wantLogUpdateNs, a.timeOfLastLogUpdateNs)
}

// enqueueSteadyLeaderCommit injects CommitPosition or NewLeadershipTerm with
// the shared notify position field (LogPosition vs CommitPosition).
func enqueueSteadyLeaderCommit(
	t *testing.T,
	q *QueueConsensusIngress,
	wire string,
	term int64,
	leader int32,
	pos int64,
) {
	t.Helper()
	switch wire {
	case "commit":
		q.Enqueue(encodeCommitPositionWire(t, codecs.CommitPositionFields{
			LeadershipTermID: term,
			LogPosition:      pos,
			LeaderMemberID:   leader,
		}))
	case "nlt":
		q.Enqueue(encodeNewLeadershipTermWire(t, codecs.NewLeadershipTermFields{
			LeadershipTermID: term,
			LeaderMemberID:   leader,
			CommitPosition:   pos,
		}))
	default:
		t.Fatalf("unknown steady wire %q", wire)
	}
}

func TestPollConsensusIngress_nilIngress_zero(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.Ingress())
	require.NotPanics(t, func() {
		require.Equal(t, 0, a.pollConsensusIngress(1))
	})
	require.Equal(t, 0, a.DoWork(1))
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
}

func TestPollConsensusIngress_electionForwards(t *testing.T) {
	runner := &recordingRunner{}
	q := &QueueConsensusIngress{}
	a := NewAgent(AgentConfig{
		Election:         runner,
		Ingress:          q,
		LeadershipTermID: 1,
	})

	appendWire := encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 1,
		LogPosition:      50,
		FollowerMemberID: 2,
	})
	commitWire := encodeCommitPositionWire(t, codecs.CommitPositionFields{
		LeadershipTermID: 1,
		LogPosition:      40,
		LeaderMemberID:   3,
	})
	q.Enqueue(appendWire)
	q.Enqueue(commitWire)

	work := a.pollConsensusIngress(100)
	require.Equal(t, 2, work)
	require.Len(t, runner.handles, 2)
	require.Equal(t, appendWire, runner.handles[0])
	require.Equal(t, commitWire, runner.handles[1])
}

func TestPollConsensusIngress_stampsLeaderUpdateClock(t *testing.T) {
	runner := &recordingRunner{}
	q := &QueueConsensusIngress{}
	a := NewAgent(AgentConfig{
		Election:         runner,
		Ingress:          q,
		LeadershipTermID: 5,
	})
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())

	// Append must not stamp the leader-update clock.
	q.Enqueue(encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 5,
		LogPosition:      10,
		FollowerMemberID: 1,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(200))
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Len(t, runner.handles, 1)

	// Commit with term >= current stamps before forward.
	q.Enqueue(encodeCommitPositionWire(t, codecs.CommitPositionFields{
		LeadershipTermID: 5,
		LogPosition:      20,
		LeaderMemberID:   1,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(200))
	require.Equal(t, int64(200), a.TimeOfLastLeaderUpdateNs())
	require.Len(t, runner.handles, 2)

	// Lower-term Commit does not move the clock.
	q.Enqueue(encodeCommitPositionWire(t, codecs.CommitPositionFields{
		LeadershipTermID: 4,
		LogPosition:      30,
		LeaderMemberID:   1,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(300))
	require.Equal(t, int64(200), a.TimeOfLastLeaderUpdateNs())
	require.Len(t, runner.handles, 3)

	// NewLeadershipTerm with term >= current stamps.
	q.Enqueue(encodeNewLeadershipTermWire(t, codecs.NewLeadershipTermFields{
		LeadershipTermID: 6,
		LeaderMemberID:   1,
		CommitPosition:   40,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(400))
	require.Equal(t, int64(400), a.TimeOfLastLeaderUpdateNs())
	require.Len(t, runner.handles, 4)
}

func TestSteadyAppendPosition_updatesMember(t *testing.T) {
	pt := &recordingPositionTable{}
	q := &QueueConsensusIngress{}
	a := NewAgent(AgentConfig{
		Role:             RoleLeader,
		Positions:        pt,
		Ingress:          q,
		LeadershipTermID: 2,
	})

	q.Enqueue(encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 2,
		LogPosition:      100,
		FollowerMemberID: 7,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(50))
	require.Equal(t, []memberPositionUpdate{{
		memberID:         7,
		leadershipTermID: 2,
		logPosition:      100,
	}}, pt.updates)

	// Lower term still updates (gate is > only, not !=).
	q.Enqueue(encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 1,
		LogPosition:      150,
		FollowerMemberID: 7,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(55))
	require.Equal(t, []memberPositionUpdate{
		{memberID: 7, leadershipTermID: 2, logPosition: 100},
		{memberID: 7, leadershipTermID: 1, logPosition: 150},
	}, pt.updates)

	// Higher term: ignore (no member update).
	q.Enqueue(encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 3,
		LogPosition:      200,
		FollowerMemberID: 7,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(60))
	require.Len(t, pt.updates, 2)

	// Non-leader: ignore.
	a.RoleSet(RoleFollower)
	q.Enqueue(encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 2,
		LogPosition:      150,
		FollowerMemberID: 8,
	}))
	require.Equal(t, 1, a.pollConsensusIngress(70))
	require.Len(t, pt.updates, 2)

	// Nil positions: no panic.
	a.RoleSet(RoleLeader)
	a.SetPositions(nil)
	q.Enqueue(encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 2,
		LogPosition:      160,
		FollowerMemberID: 9,
	}))
	require.NotPanics(t, func() {
		require.Equal(t, 1, a.pollConsensusIngress(80))
	})
}

func TestSteadyLeaderCommit_notifiesFollower(t *testing.T) {
	// CommitPosition and NewLeadershipTerm share applySteadyLeaderCommitNotify.
	tests := []struct {
		name          string
		wire          string
		term          int64
		leader        int32
		seedNotify    int64
		advancePos    int64
		lowerPos      int64
		wrongLeader   int32
		wrongPos      int64
		leaderRolePos int64
		baseNs        int64
	}{
		{
			name: "CommitPosition", wire: "commit",
			term: 3, leader: 1, seedNotify: 10,
			advancePos: 50, lowerPos: 40,
			wrongLeader: 2, wrongPos: 90, leaderRolePos: 200,
			baseNs: 1000,
		},
		{
			name: "NewLeadershipTerm", wire: "nlt",
			term: 4, leader: 2, seedNotify: 5,
			advancePos: 80, lowerPos: 60,
			wrongLeader: 9, wrongPos: 200, leaderRolePos: 300,
			baseNs: 2000,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, q := followerWithIngress(tc.term, tc.leader, tc.seedNotify)
			ns := tc.baseNs

			enqueueSteadyLeaderCommit(t, q, tc.wire, tc.term, tc.leader, tc.advancePos)
			requireSteadyPoll(t, a, ns, tc.advancePos, ns, ns)

			// Max only: lower position does not decrease notified; still stamps clocks.
			ns += 100
			enqueueSteadyLeaderCommit(t, q, tc.wire, tc.term, tc.leader, tc.lowerPos)
			requireSteadyPoll(t, a, ns, tc.advancePos, ns, ns)

			// Wrong leader id: stamp only, no notify advance.
			ns += 100
			logNs := ns - 100
			enqueueSteadyLeaderCommit(t, q, tc.wire, tc.term, tc.wrongLeader, tc.wrongPos)
			requireSteadyPoll(t, a, ns, tc.advancePos, ns, logNs)

			// Leader role: stamp only, no notify.
			a.RoleSet(RoleLeader)
			ns += 100
			enqueueSteadyLeaderCommit(t, q, tc.wire, tc.term, tc.leader, tc.leaderRolePos)
			requireSteadyPoll(t, a, ns, tc.advancePos, ns, logNs)
		})
	}
}

func TestSteadyLeaderCommit_nullLeader_skipsNotify(t *testing.T) {
	// Fail-closed has two cases: Agent leader unset, and wire leader mismatch.
	// Cover both; wire shape (CommitPosition vs NLT) is shared notify path.
	tests := []struct {
		name        string
		wire        string
		agentLeader int32
		wireLeader  int32
	}{
		// Case 1: both unset (-1). Without Agent-null case, both-null would
		// fall through mismatch check (-1 != -1) and wrongly notify.
		{
			name: "agentNull_wireNull_Commit", wire: "commit",
			agentLeader: nullMemberID, wireLeader: nullMemberID,
		},
		{
			name: "agentNull_wireNull_NLT", wire: "nlt",
			agentLeader: nullMemberID, wireLeader: nullMemberID,
		},
		// Case 2: known Agent leader, wire null sentinel.
		{
			name: "agentKnown_wireNull_Commit", wire: "commit",
			agentLeader: 1, wireLeader: nullMemberID,
		},
		{
			name: "agentKnown_wireNull_NLT", wire: "nlt",
			agentLeader: 1, wireLeader: nullMemberID,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, q := followerWithIngress(1, tc.agentLeader, 25)
			enqueueSteadyLeaderCommit(t, q, tc.wire, 1, tc.wireLeader, 100)
			requireSteadyPoll(t, a, 500, 25, 500, 0)
		})
	}
}

func TestSteadyLeaderCommit_higherTerm_noNotify(t *testing.T) {
	tests := []struct {
		name         string
		wire         string
		term         int64
		seedNotify   int64
		higherTerm   int64
		higherPos    int64
		higherLeader int32
		lowerTerm    int64
		lowerPos     int64
		baseNs       int64
	}{
		{
			name: "CommitPosition", wire: "commit",
			term: 2, seedNotify: 30,
			higherTerm: 3, higherPos: 999, higherLeader: 1,
			lowerTerm: 1, lowerPos: 1000,
			baseNs: 700,
		},
		{
			name: "NewLeadershipTerm", wire: "nlt",
			term: 1, seedNotify: 10,
			higherTerm: 5, higherPos: 500, higherLeader: 9,
			lowerTerm: 0, lowerPos: 600,
			baseNs: 3000,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, q := followerWithIngress(tc.term, 1, tc.seedNotify)

			enqueueSteadyLeaderCommit(t, q, tc.wire, tc.higherTerm, tc.higherLeader, tc.higherPos)
			requireSteadyPoll(t, a, tc.baseNs, tc.seedNotify, tc.baseNs, 0)

			// Lower term: stamp skipped, no notify.
			enqueueSteadyLeaderCommit(t, q, tc.wire, tc.lowerTerm, 1, tc.lowerPos)
			requireSteadyPoll(t, a, tc.baseNs+100, tc.seedNotify, tc.baseNs, 0)
		})
	}
}

func TestSteadyLeaderCommit_memberIDZero_validLeader(t *testing.T) {
	// Member id 0 is valid; null is only -1.
	for _, wire := range []string{"commit", "nlt"} {
		t.Run(wire, func(t *testing.T) {
			a, q := followerWithIngress(1, 0, 0)
			enqueueSteadyLeaderCommit(t, q, wire, 1, 0, 42)
			requireSteadyPoll(t, a, 10, 42, 10, 10)
		})
	}
}

func TestSteadyRequestVote_higherCandidateTerm(t *testing.T) {
	const (
		agentTerm int64 = 4
		nowNs     int64 = 700
	)
	tests := []struct {
		name          string
		role          Role
		candidateTerm int64
		noFactory     bool
		wantInstall   bool
	}{
		{
			name: "follower_higher_term", role: RoleFollower,
			candidateTerm: agentTerm + 1, wantInstall: true,
		},
		{name: "leader_higher_term", role: RoleLeader, candidateTerm: agentTerm + 2, wantInstall: true},
		{
			name: "candidate_higher_term", role: RoleCandidate,
			candidateTerm: agentTerm + 1, wantInstall: true,
		},
		{name: "equal_term", role: RoleFollower, candidateTerm: agentTerm},
		{name: "lower_term", role: RoleFollower, candidateTerm: agentTerm - 1},
		{name: "nil_factory", role: RoleFollower, candidateTerm: agentTerm + 1, noFactory: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var seeds []ElectionSeed
			var runners []*recordingRunner
			q := &QueueConsensusIngress{}
			a := NewAgent(AgentConfig{
				Role:             tc.role,
				Ingress:          q,
				ThisMemberID:     1,
				LeadershipTermID: agentTerm,
				LeaderMemberID:   1,
			})
			if !tc.noFactory {
				a.SetElectionFactory(func(seed ElectionSeed) ElectionRunner {
					seeds = append(seeds, seed)
					r := &recordingRunner{}
					runners = append(runners, r)
					return r
				})
			}

			q.Enqueue(encodeRequestVoteWire(t, codecs.RequestVoteFields{
				LogLeadershipTermID: agentTerm,
				LogPosition:         64,
				CandidateTermID:     tc.candidateTerm,
				CandidateMemberID:   2,
			}))
			require.Equal(t, 1, a.pollConsensusIngress(nowNs))

			require.Zero(t, a.TimeOfLastLeaderUpdateNs())
			require.Zero(t, a.timeOfLastLogUpdateNs)
			if !tc.wantInstall {
				require.Nil(t, a.Election())
				require.Zero(t, a.ElectionCount())
				require.Equal(t, ElectionTriggerNone, a.LastElectionTrigger())
				require.Equal(t, tc.role, a.Role())
				require.Empty(t, seeds)
				return
			}
			require.NotNil(t, a.Election())
			require.Equal(t, int64(1), a.ElectionCount())
			require.Equal(t, ElectionTriggerVoteRequest, a.LastElectionTrigger())
			require.Equal(t, RoleFollower, a.Role())
			require.Len(t, seeds, 1)
			require.Equal(t, agentTerm, seeds[0].LeadershipTermID)
			require.Equal(t, nullMemberID, seeds[0].GracefulClosedLeaderID)
			// The agent does not forward the frame to the new runner.
			require.Empty(t, runners[0].handles)
		})
	}

	t.Run("election_live", func(t *testing.T) {
		fac := &recordingElectionFactory{}
		runner := &recordingRunner{}
		q := &QueueConsensusIngress{}
		a := NewAgent(AgentConfig{
			Role:             RoleFollower,
			Ingress:          q,
			LeadershipTermID: agentTerm,
			LeaderMemberID:   1,
		})
		a.SetElection(runner)
		a.SetElectionFactory(fac.factory())

		wire := encodeRequestVoteWire(t, codecs.RequestVoteFields{
			LogLeadershipTermID: agentTerm,
			LogPosition:         64,
			CandidateTermID:     agentTerm + 1,
			CandidateMemberID:   2,
		})
		q.Enqueue(wire)
		require.Equal(t, 1, a.pollConsensusIngress(nowNs))

		require.Equal(t, ElectionRunner(runner), a.Election())
		require.Equal(t, [][]byte{wire}, runner.handles)
		require.Equal(t, int64(1), a.ElectionCount())
		require.Equal(t, ElectionTriggerBootstrap, a.LastElectionTrigger())
		require.Empty(t, fac.seeds)
	})
}

func TestQueueConsensusIngress_fragmentLimit(t *testing.T) {
	q := &QueueConsensusIngress{}
	for i := range 5 {
		q.Enqueue([]byte{byte(i)})
	}
	var seen []byte
	n := q.Poll(3, func(buf []byte, offset int) {
		require.Equal(t, 0, offset)
		require.Len(t, buf, 1)
		seen = append(seen, buf[0])
	})
	require.Equal(t, 3, n)
	require.Equal(t, []byte{0, 1, 2}, seen)

	n = q.Poll(10, func(buf []byte, offset int) {
		seen = append(seen, buf[0])
	})
	require.Equal(t, 2, n)
	require.Equal(t, []byte{0, 1, 2, 3, 4}, seen)
	require.Equal(t, 0, q.Poll(1, func([]byte, int) {
		t.Fatal("empty queue must not invoke onFragment")
	}))
}

func TestQueueConsensusIngress_nilAndNonPositiveGuards(t *testing.T) {
	var nilQ *QueueConsensusIngress
	require.NotPanics(t, func() {
		nilQ.Enqueue([]byte{1})
		require.Equal(t, 0, nilQ.Poll(1, func([]byte, int) {
			t.Fatal("nil queue must not invoke onFragment")
		}))
	})

	q := &QueueConsensusIngress{}
	for i := range 3 {
		q.Enqueue([]byte{byte(i)})
	}
	// Seeded msgs stay queued when handler is nil or fragmentLimit is non-positive.
	require.Equal(t, 0, q.Poll(1, nil))
	require.Equal(t, 0, q.Poll(0, func([]byte, int) {
		t.Fatal("zero limit must not invoke onFragment")
	}))
	require.Equal(t, 0, q.Poll(-1, func([]byte, int) {
		t.Fatal("negative limit must not invoke onFragment")
	}))
	require.Equal(t, 3, q.Poll(10, func([]byte, int) {}))
}

func TestPollConsensusIngress_agentFragmentLimit(t *testing.T) {
	q := &QueueConsensusIngress{}
	a := NewAgent(AgentConfig{Ingress: q})
	const total = defaultConsensusIngressFragmentLimit + 3
	for range total {
		q.Enqueue([]byte{1, 2, 3}) // Short frame: bad header; still one Poll fragment.
	}
	require.Equal(t, defaultConsensusIngressFragmentLimit, a.pollConsensusIngress(1))
	require.Equal(t, 3, a.pollConsensusIngress(1))
	require.Equal(t, 0, a.pollConsensusIngress(1))
}

func TestSetIngress_wireAndClear(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.Ingress())
	q := &QueueConsensusIngress{}
	a.SetIngress(q)
	require.Equal(t, ConsensusIngress(q), a.Ingress())
	a.SetIngress(nil)
	require.Nil(t, a.Ingress())
}

func TestPollConsensusIngress_badHeader_noPanic(t *testing.T) {
	q := &QueueConsensusIngress{}
	a := NewAgent(AgentConfig{Ingress: q})
	q.Enqueue([]byte{1, 2, 3}) // too short for MessageHeader
	require.NotPanics(t, func() {
		require.Equal(t, 1, a.pollConsensusIngress(1))
	})
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Equal(t, int64(0), a.NotifiedCommitPosition())
}

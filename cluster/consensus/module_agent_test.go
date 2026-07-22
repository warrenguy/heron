package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type orderRunner struct {
	n int
}

func (r *orderRunner) DoWork(int64) int { return r.n }

func (r *orderRunner) HandleConsensusMessage([]byte, int) bool { return false }

type stubPositionTable struct{}

func (s *stubPositionTable) UpdateMemberPosition(
	memberID int32,
	leadershipTermID int64,
	logPosition int64,
) {
}

func (s *stubPositionTable) QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64 {
	return appendPosition
}

// spyPositionTable records stamp/rank call order for leader consensusWork tests.
// fixedQuorum, when set, is host-minned: min(*fixedQuorum, appendPosition).
type spyPositionTable struct {
	updates     []memberPositionUpdate
	callOrder   []string
	lastNowNs   int64
	fixedQuorum *int64
}

func (s *spyPositionTable) UpdateMemberPosition(
	memberID int32,
	leadershipTermID int64,
	logPosition int64,
) {
	s.callOrder = append(s.callOrder, "update")
	s.updates = append(s.updates, memberPositionUpdate{
		memberID:         memberID,
		leadershipTermID: leadershipTermID,
		logPosition:      logPosition,
	})
}

func (s *spyPositionTable) QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64 {
	s.callOrder = append(s.callOrder, "quorum")
	s.lastNowNs = nowNs
	if s.fixedQuorum != nil {
		return min(*s.fixedQuorum, appendPosition)
	}
	return appendPosition
}

// newLeaderConsensusAgent builds a leader Agent with mode-1 append inject and
// spy positions for consensusWork unit tests.
func newLeaderConsensusAgent(
	appendPos, quorum int64,
	thisMemberID int32,
	term int64,
	intervalNs int64,
) (*Agent, *spyPositionTable, *recordingEgress, *atomic.Int64) {
	appendV := new(atomic.Int64)
	appendV.Store(appendPos)
	eg := &recordingEgress{}
	q := quorum
	pt := &spyPositionTable{fixedQuorum: &q}
	a := NewAgent(AgentConfig{
		Role:                      RoleLeader,
		Commit:                    new(atomic.Int64),
		Positions:                 pt,
		Egress:                    eg,
		AppendSource:              &AtomicAppendPosition{V: appendV},
		ThisMemberID:              thisMemberID,
		LeadershipTermID:          term,
		LeaderHeartbeatIntervalNs: intervalNs,
	})
	return a, pt, eg, appendV
}

type commitPublishCall struct {
	pos  int64
	term int64
}

type appendPublishCall struct {
	term  int64
	pos   int64
	flags uint8
}

type recordingEgress struct {
	commits      []commitPublishCall
	appends      []appendPublishCall
	refuseAppend bool // when true, PublishAppendPosition returns false
}

func (e *recordingEgress) PublishCommitPosition(commitPosition, leadershipTermID int64) {
	e.commits = append(e.commits, commitPublishCall{pos: commitPosition, term: leadershipTermID})
}

func (e *recordingEgress) PublishAppendPosition(
	leadershipTermID int64,
	logPosition int64,
	flags uint8,
) bool {
	if e.refuseAppend {
		return false
	}
	e.appends = append(e.appends, appendPublishCall{
		term:  leadershipTermID,
		pos:   logPosition,
		flags: flags,
	})
	return true
}

func TestProposeMaxCommit_monotonic(t *testing.T) {
	a := NewAgent(AgentConfig{Commit: new(atomic.Int64)})

	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, int64(10), a.ProposeMaxCommit(10))
	require.Equal(t, int64(10), a.CommitPosition())
	require.Equal(t, int64(10), a.ProposeMaxCommit(5))
	require.Equal(t, int64(10), a.CommitPosition())
	require.Equal(t, int64(20), a.ProposeMaxCommit(20))
	require.Equal(t, int64(20), a.CommitPosition())
}

func TestProposeMaxCommit_nilCommit(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, int64(0), a.ProposeMaxCommit(42))
	require.Equal(t, int64(0), a.CommitPosition())
}

func TestUpdateLeaderPosition_gateAdvancesCommit(t *testing.T) {
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Commit:           new(atomic.Int64),
		Egress:           eg,
		LeadershipTermID: 7,
	})

	require.Equal(t, 1, a.UpdateLeaderPosition(1, 50))
	require.Equal(t, int64(50), a.CommitPosition())
	require.Equal(t, int64(1), a.timeOfLastLogUpdateNs)
	require.Equal(t, []commitPublishCall{{pos: 50, term: 7}}, eg.commits)
}

func TestUpdateLeaderPosition_heartbeatRepublish(t *testing.T) {
	commit := new(atomic.Int64)
	commit.Store(50)
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Commit:                    commit,
		Egress:                    eg,
		LeaderHeartbeatIntervalNs: 100,
		LeadershipTermID:          3,
	})

	// Seed last log-update so next call needs the interval path.
	a.timeOfLastLogUpdateNs = 10

	// Below interval: no work, no publish, log-update clock unchanged.
	require.Equal(t, 0, a.UpdateLeaderPosition(50, 50))
	require.Empty(t, eg.commits)
	require.Equal(t, int64(50), a.CommitPosition())
	require.Equal(t, int64(10), a.timeOfLastLogUpdateNs)

	require.Equal(t, 1, a.UpdateLeaderPosition(110, 50))
	require.Equal(t, int64(50), a.CommitPosition())
	require.Equal(t, int64(110), a.timeOfLastLogUpdateNs)
	require.Equal(t, []commitPublishCall{{pos: 50, term: 3}}, eg.commits)
}

func TestUpdateLeaderPosition_nilCommit_gateWork(t *testing.T) {
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Egress:                    eg,
		LeaderHeartbeatIntervalNs: 10,
		LeadershipTermID:          2,
	})

	// Quorum above nil-commit (0) opens the gate; ProposeMax is a no-op.
	require.Equal(t, 1, a.UpdateLeaderPosition(1, 50))
	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, int64(1), a.timeOfLastLogUpdateNs)
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Equal(t, []commitPublishCall{{pos: 50, term: 2}}, eg.commits)

	// Nil commit reads as 0, so any positive quorum keeps the gate open.
	// Work 0 only when quorum <= 0 and interval is not due.
	require.Equal(t, 0, a.UpdateLeaderPosition(5, 0))
	require.Equal(t, []commitPublishCall{{pos: 50, term: 2}}, eg.commits)
	require.Equal(t, int64(1), a.timeOfLastLogUpdateNs)

	// Interval due re-publishes even with nil commit and flat/zero quorum.
	require.Equal(t, 1, a.UpdateLeaderPosition(11, 0))
	require.Equal(t, int64(11), a.timeOfLastLogUpdateNs)
	require.Equal(t, []commitPublishCall{{pos: 50, term: 2}, {pos: 0, term: 2}}, eg.commits)
}

func TestUpdateLeaderPosition_nilCommit_nilEgress_gateWork(t *testing.T) {
	a := NewAgent(AgentConfig{LeaderHeartbeatIntervalNs: 10})

	require.Equal(t, 1, a.UpdateLeaderPosition(1, 50))
	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, int64(1), a.timeOfLastLogUpdateNs)
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())

	// Work 0: quorum <= nil-commit (0) and interval not due.
	require.Equal(t, 0, a.UpdateLeaderPosition(5, 0))
	require.Equal(t, int64(1), a.timeOfLastLogUpdateNs)

	// Interval due still returns work 1 with nil egress (no panic, clock moves).
	require.Equal(t, 1, a.UpdateLeaderPosition(11, 0))
	require.Equal(t, int64(11), a.timeOfLastLogUpdateNs)
}

func TestUpdateLeaderPosition_advancesCommit_workOne(t *testing.T) {
	a := NewAgent(AgentConfig{Commit: new(atomic.Int64)})
	require.Equal(t, 1, a.UpdateLeaderPosition(1, 50))
	require.Equal(t, int64(50), a.CommitPosition())
	require.Equal(t, 0, a.UpdateLeaderPosition(2, 50))
	require.Equal(t, int64(50), a.CommitPosition())
	require.Equal(t, 1, a.UpdateLeaderPosition(3, 75))
	require.Equal(t, int64(75), a.CommitPosition())
	require.Equal(t, int64(100), a.ProposeMaxCommit(100))
	require.Equal(t, 0, a.UpdateLeaderPosition(4, 50))
	require.Equal(t, int64(100), a.CommitPosition())
}

func TestUpdateLeaderPosition_doesNotWriteTimeOfLastLeaderUpdateNs(t *testing.T) {
	a := NewAgent(AgentConfig{Commit: new(atomic.Int64)})
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Equal(t, 1, a.UpdateLeaderPosition(9_000_000_000, 50))
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	a.timeOfLastLeaderUpdateNs = 42
	require.Equal(t, 0, a.UpdateLeaderPosition(9_000_000_001, 50))
	require.Equal(t, int64(42), a.TimeOfLastLeaderUpdateNs())
}

func TestUpdateLeaderPosition_setsTimeOfLastLogUpdateNs(t *testing.T) {
	a := NewAgent(AgentConfig{Commit: new(atomic.Int64), LeaderHeartbeatIntervalNs: 100})
	require.Equal(t, int64(0), a.timeOfLastLogUpdateNs)

	require.Equal(t, 1, a.UpdateLeaderPosition(25, 10))
	require.Equal(t, int64(25), a.timeOfLastLogUpdateNs)

	// Flat quorum before interval: clock unchanged.
	require.Equal(t, 0, a.UpdateLeaderPosition(50, 10))
	require.Equal(t, int64(25), a.timeOfLastLogUpdateNs)

	// Heartbeat path advances the log-update clock only.
	require.Equal(t, 1, a.UpdateLeaderPosition(125, 10))
	require.Equal(t, int64(125), a.timeOfLastLogUpdateNs)
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
}

func TestUpdateLeaderPosition_commitNeverDecreases(t *testing.T) {
	a := NewAgent(AgentConfig{Commit: new(atomic.Int64), LeaderHeartbeatIntervalNs: 1})

	quorums := []int64{10, 5, 20, 15, 30, 0, 40}
	now := int64(0)
	for _, q := range quorums {
		prev := a.CommitPosition()

		// Force gate open each step via interval (now advances by interval).
		now += a.leaderHeartbeatIntervalNs
		require.Equal(t, 1, a.UpdateLeaderPosition(now, q))
		require.Equal(t, max(prev, q), a.CommitPosition())
	}
	require.Equal(t, int64(40), a.CommitPosition())
}

func TestUpdateLeaderPosition_heartbeatPublishesBelowCommit(t *testing.T) {
	// Interval-due gate publishes wire quorum even when below commit;
	// ProposeMax keeps commit non-decreasing.
	commit := new(atomic.Int64)
	commit.Store(50)
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Commit:                    commit,
		Egress:                    eg,
		LeaderHeartbeatIntervalNs: 100,
		LeadershipTermID:          4,
	})
	a.timeOfLastLogUpdateNs = 10

	require.Equal(t, 1, a.UpdateLeaderPosition(110, 40))
	require.Equal(t, int64(50), a.CommitPosition())
	require.Equal(t, int64(110), a.timeOfLastLogUpdateNs)
	require.Equal(t, []commitPublishCall{{pos: 40, term: 4}}, eg.commits)
}

func TestSetThisMemberID(t *testing.T) {
	a := NewAgent(AgentConfig{ThisMemberID: 5})
	require.Equal(t, int32(5), a.ThisMemberID())

	a.SetThisMemberID(7)
	require.Equal(t, int32(7), a.ThisMemberID())

	// Zero is a valid member id, not only at construct.
	a.SetThisMemberID(0)
	require.Equal(t, int32(0), a.ThisMemberID())

	a.SetThisMemberID(nullMemberID)
	require.Equal(t, nullMemberID, a.ThisMemberID())
}

func TestSetLeaderMemberID(t *testing.T) {
	a := NewAgent(AgentConfig{LeaderMemberID: 5})
	require.Equal(t, int32(5), a.LeaderMemberID())

	a.SetLeaderMemberID(7)
	require.Equal(t, int32(7), a.LeaderMemberID())

	// Zero is a valid member id, not only at construct.
	a.SetLeaderMemberID(0)
	require.Equal(t, int32(0), a.LeaderMemberID())

	a.SetLeaderMemberID(nullMemberID)
	require.Equal(t, nullMemberID, a.LeaderMemberID())
}

func TestJoinLogAsLeader_setsTermAndClearsLeaderTarget(t *testing.T) {
	a := NewAgent(AgentConfig{
		Egress:           &recordingEgress{},
		LeaderMemberID:   2,
		LeadershipTermID: 1,
	})

	a.JoinLogAsLeader(9, 100, 4, true)
	require.Equal(t, RoleLeader, a.Role())
	require.Equal(t, int64(9), a.LeadershipTermID())
	require.Equal(t, nullMemberID, a.LeaderMemberID())
}

func TestNewAgent_memberIDsZeroValid(t *testing.T) {
	// Zero is a valid member id; constructor must not rewrite to -1.
	a := NewAgent(AgentConfig{})
	require.Equal(t, int32(0), a.ThisMemberID())
	require.Equal(t, int32(0), a.LeaderMemberID())

	aUnset := NewAgent(AgentConfig{
		ThisMemberID:   nullMemberID,
		LeaderMemberID: nullMemberID,
	})
	require.Equal(t, nullMemberID, aUnset.ThisMemberID())
	require.Equal(t, nullMemberID, aUnset.LeaderMemberID())
}

func TestNewAgent_negativeHeartbeatIntervalPanics(t *testing.T) {
	require.Panics(t, func() {
		_ = NewAgent(AgentConfig{LeaderHeartbeatIntervalNs: -1})
	})
}

func TestSetPositions_andSetEgress_wireAndClear(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.Positions())
	require.Nil(t, a.Egress())

	pt := &stubPositionTable{}
	eg := &recordingEgress{}
	a.SetPositions(pt)
	a.SetEgress(eg)
	require.Equal(t, PositionTable(pt), a.Positions())
	require.Equal(t, ConsensusEgress(eg), a.Egress())

	a.SetPositions(nil)
	a.SetEgress(nil)
	require.Nil(t, a.Positions())
	require.Nil(t, a.Egress())
}

func TestSetElection_andGetter(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.Election())
	r := &orderRunner{n: 1}
	a.SetElection(r)
	require.Equal(t, ElectionRunner(r), a.Election())
	a.SetElection(nil)
	require.Nil(t, a.Election())
}

func TestHostDelegate_stubsSmoke(t *testing.T) {
	a := NewAgent(AgentConfig{})

	require.Equal(t, RoleFollower, a.Role())
	a.RoleSet(RoleCandidate)
	require.Equal(t, RoleCandidate, a.Role())

	a.LiveLogDestinationSet("live")
	require.Equal(t, "live", a.LiveLogDestination())
	a.CatchupLogDestinationSet("catchup")
	require.Equal(t, "catchup", a.CatchupLogDestination())

	require.Equal(t, int32(-1), a.AddLogPublication(100))
	require.Equal(t, int64(-1), a.LocalLogRecordingID())
	a.SetLogRecordingID(99)
	require.Equal(t, int64(99), a.LocalLogRecordingID())

	params, ready := a.FollowerLogJoinReady(7)
	require.False(t, ready)
	require.Equal(t, FollowerLogJoinParams{}, params)

	replay := a.NewLogReplay(10, 200)
	require.NotNil(t, replay)
	require.True(t, replay.IsDone())
	require.Equal(t, int64(200), replay.Position())
	require.Equal(t, 0, replay.DoWork(1))

	require.Nil(t, a.NewLogReplication("leader", "response", 1, 2, 3))
	require.Equal(t, 0, a.CatchupPoll(0, 0))
	a.CatchupInitiated(0)
	a.StopAllCatchups()
	require.False(t, a.IsCatchupNearLive(0))
	require.False(t, a.AppendNewLeadershipTermEvent(0))
	require.Equal(t, 0, a.PollArchiveEvents())
	a.TruncateLogEntry(0, 0)
	a.AwaitLocalSocketsClosed(0)
}

func TestAgentHandler_prepareJoinComplete(t *testing.T) {
	r := &orderRunner{n: 1}
	pt := &stubPositionTable{}
	eg := &recordingEgress{}
	commit := new(atomic.Int64)
	commit.Store(77)
	a := NewAgent(AgentConfig{
		Election:         r,
		Positions:        pt,
		Egress:           eg,
		Commit:           commit,
		Role:             RoleLeader,
		LeaderMemberID:   9,
		LeadershipTermID: 0,
	})
	a.lastPublishedAppendPosition = 50
	a.timeOfLastAppendPositionSendNs = 100

	got := a.PrepareForNewLeadership(1234, 99)
	require.Equal(t, int64(1234), got)
	require.Equal(t, RoleFollower, a.Role())
	require.Equal(t, nullMemberID, a.LeaderMemberID())
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)
	require.Equal(t, int64(0), a.timeOfLastAppendPositionSendNs)
	// Commit is kept across prepare.
	require.Equal(t, int64(77), a.CommitPosition())

	a.JoinLogAsLeader(1, 10, 2, true)
	require.Equal(t, RoleLeader, a.Role())
	require.Equal(t, int64(1), a.LeadershipTermID())
	require.Equal(t, nullMemberID, a.LeaderMemberID())

	require.False(t, a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 3}, 1))

	a.ElectionComplete(1)
	require.Nil(t, a.Election())
	require.Equal(t, PositionTable(pt), a.Positions())
	require.Equal(t, ConsensusEgress(eg), a.Egress())
}

func TestNewAgent_defaults(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, RoleFollower, a.Role())
	require.Equal(t, int64(-1), a.LocalLogRecordingID())
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Equal(t, DefaultLeaderHeartbeatIntervalNs, a.leaderHeartbeatIntervalNs)
	require.Equal(t, int64(0), a.LeadershipTermID())
	require.Equal(t, int64(0), a.NotifiedCommitPosition())
	require.Nil(t, a.Election())
	require.Nil(t, a.Positions())
	require.Nil(t, a.Egress())
	require.Nil(t, a.AppendSource())

	require.Equal(t, RoleLeader, NewAgent(AgentConfig{Role: RoleLeader}).Role())
}

func TestLeaderConsensusWork_rankAndPublish(t *testing.T) {
	a, pt, eg, _ := newLeaderConsensusAgent(100, 80, 1, 5, 100)

	require.Equal(t, 1, a.consensusWork(1))
	require.Equal(t, int64(80), a.CommitPosition())
	require.Equal(t, []commitPublishCall{{pos: 80, term: 5}}, eg.commits)
	require.Equal(t, []memberPositionUpdate{{
		memberID:         1,
		leadershipTermID: 5,
		logPosition:      100,
	}}, pt.updates)
	require.Equal(t, []string{"update", "quorum"}, pt.callOrder)
	require.Equal(t, int64(1), pt.lastNowNs)
}

func TestLeaderConsensusWork_gateClosed_noWork(t *testing.T) {
	// First tick advances commit; second tick with flat quorum inside interval
	// still stamps + ranks, but gate returns 0 and does not re-publish.
	a, pt, eg, _ := newLeaderConsensusAgent(100, 80, 1, 5, 100)

	require.Equal(t, 1, a.consensusWork(1))
	require.Equal(t, int64(80), a.CommitPosition())
	require.Equal(t, int64(1), a.timeOfLastLogUpdateNs)
	require.Len(t, eg.commits, 1)
	require.Equal(t, []string{"update", "quorum"}, pt.callOrder)

	// Same append/quorum, now still inside heartbeat interval.
	require.Equal(t, 0, a.consensusWork(50))
	require.Equal(t, int64(80), a.CommitPosition())
	require.Equal(t, int64(1), a.timeOfLastLogUpdateNs)
	require.Len(t, eg.commits, 1)
	// Stamp + rank still ran on the closed-gate tick.
	require.Equal(t, []string{"update", "quorum", "update", "quorum"}, pt.callOrder)
	require.Len(t, pt.updates, 2)
	require.Equal(t, int64(50), pt.lastNowNs)
}

func TestLeaderConsensusWork_nullThisMember_stillRanks(t *testing.T) {
	a, pt, eg, _ := newLeaderConsensusAgent(60, 60, nullMemberID, 2, 100)

	require.Equal(t, 1, a.consensusWork(1))
	require.Empty(t, pt.updates)
	require.Equal(t, []string{"quorum"}, pt.callOrder)
	require.Equal(t, int64(60), a.CommitPosition())
	require.Equal(t, []commitPublishCall{{pos: 60, term: 2}}, eg.commits)
}

func TestLeaderConsensusWork_nilAppendSource_zero(t *testing.T) {
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Role:      RoleLeader,
		Commit:    new(atomic.Int64),
		Positions: &stubPositionTable{},
		Egress:    eg,
	})
	require.Equal(t, 0, a.consensusWork(1))
	require.Empty(t, eg.commits)
	require.Equal(t, int64(0), a.CommitPosition())
}

func TestLeaderConsensusWork_nilPositions_zero(t *testing.T) {
	appendV := new(atomic.Int64)
	appendV.Store(50)
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Role:         RoleLeader,
		Commit:       new(atomic.Int64),
		Egress:       eg,
		AppendSource: &AtomicAppendPosition{V: appendV},
	})
	require.Equal(t, 0, a.consensusWork(1))
	require.Empty(t, eg.commits)
	require.Equal(t, int64(0), a.CommitPosition())
}

func TestFollowerConsensusWork_appendInterval(t *testing.T) {
	appendV := new(atomic.Int64)
	appendV.Store(40)
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Role:                      RoleFollower,
		Egress:                    eg,
		AppendSource:              &AtomicAppendPosition{V: appendV},
		LeadershipTermID:          3,
		LeaderHeartbeatIntervalNs: 100,
		LeaderMemberID:            2,
	})

	// Advance path: first publish at now=0 with advanced position.
	require.Equal(t, 1, a.consensusWork(0))
	require.Equal(t, []appendPublishCall{{term: 3, pos: 40, flags: 0}}, eg.appends)
	require.Equal(t, int64(40), a.lastPublishedAppendPosition)
	require.Equal(t, int64(0), a.timeOfLastAppendPositionSendNs)

	// Flat position, inside interval: no work.
	require.Equal(t, 0, a.consensusWork(50))
	require.Len(t, eg.appends, 1)

	// Interval due re-publishes same position.
	require.Equal(t, 1, a.consensusWork(100))
	require.Equal(t, []appendPublishCall{
		{term: 3, pos: 40, flags: 0},
		{term: 3, pos: 40, flags: 0},
	}, eg.appends)
	require.Equal(t, int64(40), a.lastPublishedAppendPosition)
	require.Equal(t, int64(100), a.timeOfLastAppendPositionSendNs)

	// Further advance publishes new high-water without waiting full interval.
	appendV.Store(55)
	require.Equal(t, 1, a.consensusWork(110))
	require.Equal(t, appendPublishCall{term: 3, pos: 55, flags: 0}, eg.appends[2])
	require.Equal(t, int64(55), a.lastPublishedAppendPosition)
}

func TestFollowerConsensusWork_noLeader_noPublish(t *testing.T) {
	// Agent freezes cadence when PublishAppendPosition returns false
	// (HostAdapter does that for null/self leader; unit models the refuse only).
	appendV := new(atomic.Int64)
	appendV.Store(25)
	eg := &recordingEgress{refuseAppend: true}
	a := NewAgent(AgentConfig{
		Role:                      RoleFollower,
		Egress:                    eg,
		AppendSource:              &AtomicAppendPosition{V: appendV},
		LeadershipTermID:          1,
		LeaderHeartbeatIntervalNs: 10,
	})

	// Cold refuse: advance and interval both leave state at zero.
	require.Equal(t, 0, a.consensusWork(0))
	require.Empty(t, eg.appends)
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)
	require.Equal(t, int64(0), a.timeOfLastAppendPositionSendNs)
	require.Equal(t, 0, a.consensusWork(100))
	require.Empty(t, eg.appends)

	// Mid-stream: one success, then refuse freezes lastPublished and send clock.
	eg.refuseAppend = false
	require.Equal(t, 1, a.consensusWork(200))
	require.Len(t, eg.appends, 1)
	require.Equal(t, int64(25), a.lastPublishedAppendPosition)
	require.Equal(t, int64(200), a.timeOfLastAppendPositionSendNs)

	eg.refuseAppend = true
	appendV.Store(40)
	require.Equal(t, 0, a.consensusWork(205))
	require.Len(t, eg.appends, 1)
	require.Equal(t, int64(25), a.lastPublishedAppendPosition)
	require.Equal(t, int64(200), a.timeOfLastAppendPositionSendNs)
	require.Equal(t, 0, a.consensusWork(300))
	require.Len(t, eg.appends, 1)
	require.Equal(t, int64(25), a.lastPublishedAppendPosition)
	require.Equal(t, int64(200), a.timeOfLastAppendPositionSendNs)
}

func TestFollowerConsensusWork_nilAppendSource_intervalPublish(t *testing.T) {
	// Nil source records 0; interval re-publish still uses
	// max(0, lastPublishedAppendPosition) when egress accepts.
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Role:                      RoleFollower,
		Egress:                    eg,
		LeadershipTermID:          4,
		LeaderHeartbeatIntervalNs: 10,
		LeaderMemberID:            2,
	})
	require.Nil(t, a.AppendSource())

	// Cold start: not advanced (0 not > 0); interval due at now >= interval.
	require.Equal(t, 1, a.consensusWork(10))
	require.Equal(t, []appendPublishCall{{term: 4, pos: 0, flags: 0}}, eg.appends)
	// Interval path does not raise lastPublished when position is not advanced.
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)
	require.Equal(t, int64(10), a.timeOfLastAppendPositionSendNs)

	// Seed lastPublished; nil source re-publishes that high-water on interval.
	a.lastPublishedAppendPosition = 15
	require.Equal(t, 1, a.consensusWork(20))
	require.Equal(t, appendPublishCall{term: 4, pos: 15, flags: 0}, eg.appends[1])
	require.Equal(t, int64(15), a.lastPublishedAppendPosition)
	require.Equal(t, int64(20), a.timeOfLastAppendPositionSendNs)
}

func TestFollowerConsensusWork_nilEgress_zero(t *testing.T) {
	appendV := new(atomic.Int64)
	appendV.Store(10)
	a := NewAgent(AgentConfig{
		Role:         RoleFollower,
		AppendSource: &AtomicAppendPosition{V: appendV},
	})
	require.Equal(t, 0, a.consensusWork(0))
}

func TestSetAppendSource_wireAndClear(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.AppendSource())

	src := &AtomicAppendPosition{V: new(atomic.Int64)}
	a.SetAppendSource(src)
	require.Equal(t, AppendPositionSource(src), a.AppendSource())

	a.SetAppendSource(nil)
	require.Nil(t, a.AppendSource())
}

func TestAtomicAppendPosition_nilSafe(t *testing.T) {
	require.Equal(t, int64(0), (*AtomicAppendPosition)(nil).AppendPosition())
	require.Equal(t, int64(0), (&AtomicAppendPosition{}).AppendPosition())
	v := new(atomic.Int64)
	v.Store(9)
	require.Equal(t, int64(9), (&AtomicAppendPosition{V: v}).AppendPosition())
}

package consensus

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// fakeLogTruncateControl records TruncateLogRecording calls for agent tests.
type fakeLogTruncateControl struct {
	calls                   []fakeTruncateCall
	err                     error
	nextDeletedSegmentCount int64
}

type fakeTruncateCall struct {
	recordingID int64
	position    int64
}

var _ LogTruncateControl = (*fakeLogTruncateControl)(nil)

func (f *fakeLogTruncateControl) TruncateLogRecording(
	recordingID int64,
	position int64,
) (int64, error) {
	f.calls = append(f.calls, fakeTruncateCall{
		recordingID: recordingID,
		position:    position,
	})
	if f.err != nil {
		return 0, f.err
	}
	return f.nextDeletedSegmentCount, nil
}

// orderRunner returns a fixed work count and counts DoWork calls.
type orderRunner struct {
	n     int
	calls int
}

func (r *orderRunner) DoWork(int64) int {
	r.calls++
	return r.n
}

func (r *orderRunner) HandleConsensusMessage([]byte, int) bool { return false }

func (r *orderRunner) HandleReplayNewLeadershipTermEvent(
	codecs.NewLeadershipTermEventFields,
) error {
	return nil
}

var _ PositionTable = (*stubPositionTable)(nil)

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

func (s *stubPositionTable) HasActiveQuorum(nowNs, timeoutNs int64) bool { return true }

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

func (s *spyPositionTable) HasActiveQuorum(nowNs, timeoutNs int64) bool { return true }

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

type stopCatchupPublishCall struct {
	term       int64
	followerID int32
}

var _ ConsensusEgress = (*recordingEgress)(nil)

type recordingEgress struct {
	commits      []commitPublishCall
	appends      []appendPublishCall
	stopCatchups []stopCatchupPublishCall
	answers      []CanvassAnswer
	refuseAppend bool // when true, PublishAppendPosition returns false
	// refuseStopCatchup, when true, PublishStopCatchup returns false.
	refuseStopCatchup bool
	// refuseTerm, when true, PublishNewLeadershipTerm returns false.
	// Default false so acceptance tests see true without configuring.
	refuseTerm bool
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

func (e *recordingEgress) PublishStopCatchup(leadershipTermID int64, followerMemberID int32) bool {
	if e.refuseStopCatchup {
		return false
	}
	e.stopCatchups = append(e.stopCatchups, stopCatchupPublishCall{
		term:       leadershipTermID,
		followerID: followerMemberID,
	})
	return true
}

func (e *recordingEgress) PublishNewLeadershipTerm(answer CanvassAnswer) bool {
	e.answers = append(e.answers, answer)
	return !e.refuseTerm
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
	//
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

	require.NoError(t, a.JoinLogAsLeader(9, 100, 4, true))
	require.Equal(t, RoleLeader, a.Role())
	require.Equal(t, int64(9), a.LeadershipTermID())
	require.Equal(t, nullMemberID, a.LeaderMemberID())
	require.Equal(t, int64(100), a.termBaseLogPosition)
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

func TestNewAgent_negativeHeartbeatTimeoutPanics(t *testing.T) {
	require.Panics(t, func() {
		_ = NewAgent(AgentConfig{LeaderHeartbeatTimeoutNs: -1})
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
	// Construct inject skips installElection. ElectionCount stays 0 and
	// LastElectionTrigger stays ElectionTriggerNone.
	injected := NewAgent(AgentConfig{Election: &orderRunner{n: 1}})
	require.NotNil(t, injected.Election())
	require.Equal(t, int64(0), injected.ElectionCount())
	require.Equal(t, ElectionTriggerNone, injected.LastElectionTrigger())

	a := NewAgent(AgentConfig{})
	require.Nil(t, a.Election())
	require.Equal(t, int64(0), a.ElectionCount())
	require.Equal(t, ElectionTriggerNone, a.LastElectionTrigger())

	r := &orderRunner{n: 1}
	a.SetElection(r)
	require.Equal(t, ElectionRunner(r), a.Election())
	require.Equal(t, int64(1), a.ElectionCount())
	require.Equal(t, ElectionTriggerBootstrap, a.LastElectionTrigger())

	a.SetElection(nil)
	require.Nil(t, a.Election())
	require.Equal(t, int64(1), a.ElectionCount())
	require.Equal(t, ElectionTriggerBootstrap, a.LastElectionTrigger())
}

func TestSetElectionFactory_wireAndClear(t *testing.T) {
	const timeoutNs = DefaultLeaderHeartbeatTimeoutNs
	a := NewAgent(AgentConfig{
		Role:                     RoleFollower,
		LeaderHeartbeatTimeoutNs: timeoutNs,
	})
	a.timeOfLastLogUpdateNs = 0
	a.SetElectionFactory(func(ElectionSeed) ElectionRunner {
		return &orderRunner{n: 1}
	})
	require.Positive(t, a.DoWork(timeoutNs))
	require.NotNil(t, a.Election())
	require.Equal(t, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
	count := a.ElectionCount()

	a.ElectionComplete(timeoutNs)
	a.SetElectionFactory(nil)
	require.Equal(t, 0, a.DoWork(timeoutNs*2+slowTickIntervalNs))
	require.Nil(t, a.Election())
	require.Equal(t, count, a.ElectionCount())
}

func TestHostDelegate_stubsSmoke(t *testing.T) {
	a := NewAgent(AgentConfig{})

	require.Equal(t, RoleFollower, a.Role())
	a.RoleSet(RoleCandidate)
	require.Equal(t, RoleCandidate, a.Role())

	sessionID, err := a.AddLogPublication(100)
	require.NoError(t, err)
	require.Equal(t, int32(-1), sessionID)
	require.Equal(t, int64(-1), a.LocalLogRecordingID())
	a.SetLogRecordingID(99)
	require.Equal(t, int64(99), a.LocalLogRecordingID())
	// A START-signal id write is not a recording start.
	require.False(t, a.IsRecordingActive())

	// Bare Agent: nil LogJoinSource, so FollowerLogJoinReady reports not-ready.
	params, ready := a.FollowerLogJoinReady(7)
	require.False(t, ready)
	require.Equal(t, FollowerLogJoinParams{}, params)

	replay := a.NewLogReplay(10, 200)
	require.NotNil(t, replay)
	require.True(t, replay.IsDone())
	require.Equal(t, int64(200), replay.Position())
	require.Equal(t, 0, replay.DoWork(1))

	require.Nil(t, a.NewLogReplication("leader", "response", 1, 2, 3))
	require.Equal(t, 0, catchupPoll(t, a, 0, 0))
	a.CatchupInitiated(0)
	a.StopAllCatchups()
	require.False(t, a.IsCatchupNearLive(0))
	require.False(t, a.IsLogJoinedForSession(0))
	require.False(t, a.AppendNewLeadershipTermEvent(0))
	require.Equal(t, 0, a.PollArchiveEvents())
	require.Error(t, a.TruncateLogEntry(0, 0))
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

	got, prepared, err := a.PrepareForNewLeadership(1234, 99)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, int64(1234), got)
	require.Equal(t, RoleFollower, a.Role())
	require.Equal(t, nullMemberID, a.LeaderMemberID())
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)
	require.Equal(t, int64(0), a.timeOfLastAppendPositionSendNs)
	// Commit is kept across prepare.
	require.Equal(t, int64(77), a.CommitPosition())

	require.NoError(t, a.JoinLogAsLeader(1, 10, 2, true))
	require.Equal(t, RoleLeader, a.Role())
	require.Equal(t, int64(1), a.LeadershipTermID())
	require.Equal(t, nullMemberID, a.LeaderMemberID())

	// Bare Agent: nil LogJoinSource, so JoinLogAsFollower reports not joined.
	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 3}, 1)
	require.NoError(t, err)
	require.False(t, joined)

	a.ElectionComplete(1)
	require.Nil(t, a.Election())
	require.Equal(t, PositionTable(pt), a.Positions())
	require.Equal(t, ConsensusEgress(eg), a.Egress())
}

func TestNewAgent_defaults(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, RoleFollower, a.Role())
	require.Equal(t, int64(-1), a.LocalLogRecordingID())
	require.False(t, a.IsRecordingActive())
	require.Nil(t, a.LogRecordingControl())
	require.Equal(t, int64(0), a.TimeOfLastLeaderUpdateNs())
	require.Equal(t, DefaultLeaderHeartbeatIntervalNs, a.leaderHeartbeatIntervalNs)
	require.Equal(t, DefaultLeaderHeartbeatTimeoutNs, a.leaderHeartbeatTimeoutNs)
	require.Equal(t, int64(0), a.LeadershipTermID())
	require.Equal(t, int64(0), a.NotifiedCommitPosition())
	require.Nil(t, a.Election())
	require.Nil(t, a.Positions())
	require.Nil(t, a.Egress())
	require.Nil(t, a.AppendSource())
	require.Equal(t, int64(0), a.ElectionCount())
	require.Equal(t, ElectionTriggerNone, a.LastElectionTrigger())

	require.Equal(t, RoleLeader, NewAgent(AgentConfig{Role: RoleLeader}).Role())
}

func TestNewAgent_appendNotReadyDefaults(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, DefaultAppendPositionNotReadyTimeoutNs, a.appendNotReadyTimeoutNs)
	require.Equal(t, int64(nullValue), a.appendNotReadySinceNs)
	require.NoError(t, a.AppendRankFailure())

	const custom int64 = 123
	a = NewAgent(AgentConfig{AppendPositionNotReadyTimeoutNs: custom})
	require.Equal(t, custom, a.appendNotReadyTimeoutNs)

	require.Panics(t, func() {
		_ = NewAgent(AgentConfig{AppendPositionNotReadyTimeoutNs: -1})
	})
}

// scriptedAppendResult is one queued (position, ready) report.
type scriptedAppendResult struct {
	position int64
	ready    bool
}

// scriptedAppendSource reports a queued sequence of (position, ready) results.
// A policy test can drive a not-ready run without a counters reader. A read past
// the queue panics: the script and the read count must match.
type scriptedAppendSource struct {
	results []scriptedAppendResult
	reads   int
}

func (s *scriptedAppendSource) AppendPosition() (int64, bool) {
	r := s.results[s.reads]
	s.reads++
	return r.position, r.ready
}

func TestResolveAppendPosition_runClearsOnReady(t *testing.T) {
	const timeoutNs int64 = 1_000
	src := &scriptedAppendSource{
		results: []scriptedAppendResult{
			{0, false}, // open run
			{50, true}, // clear run
			{0, false}, // fresh run
			{0, false}, // still under bound from second start
		},
	}
	control := &fakeLogRecordingControl{active: true}
	a := NewAgent(AgentConfig{
		Role:                            RoleFollower,
		AppendSource:                    src,
		AppendPositionNotReadyTimeoutNs: timeoutNs,
	})
	a.SetLogRecordingID(77)
	a.SetLogRecordingControl(control)

	pos, ready := a.resolveAppendPosition(100)
	require.False(t, ready)
	require.Equal(t, int64(0), pos)
	require.Equal(t, int64(100), a.appendNotReadySinceNs)
	require.NoError(t, a.AppendRankFailure())

	pos, ready = a.resolveAppendPosition(200)
	require.True(t, ready)
	require.Equal(t, int64(50), pos)
	require.Equal(t, int64(nullValue), a.appendNotReadySinceNs)

	// Fresh not-ready run starts at this tick, not the earlier 100.
	pos, ready = a.resolveAppendPosition(300)
	require.False(t, ready)
	require.Equal(t, int64(0), pos)
	require.Equal(t, int64(300), a.appendNotReadySinceNs)

	// Bound measured from 300: 300+999 still under timeoutNs=1000.
	_, ready = a.resolveAppendPosition(300 + timeoutNs - 1)
	require.False(t, ready)
	require.NoError(t, a.AppendRankFailure())
	require.Equal(t, int64(300), a.appendNotReadySinceNs)
}

func TestResolveAppendPosition_failsOnlyWhileRecordingActive(t *testing.T) {
	const timeoutNs int64 = 100
	// Identical timing: active marks; inactive never does.
	for _, tc := range []struct {
		name   string
		active bool
		want   bool
	}{
		{name: "active", active: true, want: true},
		{name: "inactive", active: false, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &scriptedAppendSource{
				results: []scriptedAppendResult{{0, false}, {0, false}},
			}
			a := NewAgent(AgentConfig{
				Role:                            RoleFollower,
				AppendSource:                    src,
				AppendPositionNotReadyTimeoutNs: timeoutNs,
			})
			a.SetLogRecordingID(77)
			a.SetLogRecordingControl(&fakeLogRecordingControl{active: tc.active})

			_, ready := a.resolveAppendPosition(0)
			require.False(t, ready)
			require.NoError(t, a.AppendRankFailure())

			_, ready = a.resolveAppendPosition(timeoutNs)
			require.False(t, ready)
			if tc.want {
				require.Error(t, a.AppendRankFailure())
				require.ErrorIs(t, a.AppendRankFailure(), ErrAppendPositionUnavailable)
			} else {
				require.NoError(t, a.AppendRankFailure())
			}
		})
	}
}

// Asserts a marked append-rank failure freezes rank and publish even after the
// source reports ready.
func TestResolveAppendPosition_markedFreezesReady(t *testing.T) {
	const timeoutNs int64 = 100
	src := &scriptedAppendSource{
		results: []scriptedAppendResult{
			{0, false},   // open run
			{0, false},   // past bound: mark failure
			{9600, true}, // source recovers; must stay frozen
		},
	}
	pt := &spyPositionTable{}
	eg := &recordingEgress{}
	a := NewAgent(AgentConfig{
		Role:                            RoleLeader,
		Commit:                          new(atomic.Int64),
		Positions:                       pt,
		Egress:                          eg,
		AppendSource:                    src,
		AppendPositionNotReadyTimeoutNs: timeoutNs,
		ThisMemberID:                    1,
		LeadershipTermID:                3,
	})
	a.SetLogRecordingID(77)
	a.SetLogRecordingControl(&fakeLogRecordingControl{active: true})

	_, ready := a.resolveAppendPosition(0)
	require.False(t, ready)
	require.NoError(t, a.AppendRankFailure())

	_, ready = a.resolveAppendPosition(timeoutNs)
	require.False(t, ready)
	marked := a.AppendRankFailure()
	require.Error(t, marked)
	require.ErrorIs(t, marked, ErrAppendPositionUnavailable)

	// Source would report ready; marked failure must freeze.
	pos, ready := a.resolveAppendPosition(timeoutNs * 2)
	require.False(t, ready)
	require.Equal(t, int64(0), pos)
	require.Equal(t, marked, a.AppendRankFailure())
	// Marked path skips the source: queue must not advance.
	require.Equal(t, 2, src.reads)

	// Leader: no stamp, no rank work after mark.
	require.Equal(t, 0, a.consensusWork(timeoutNs*3))
	require.Empty(t, pt.updates)
	require.Empty(t, pt.callOrder)
	require.Equal(t, int64(0), a.CommitPosition())
	require.Equal(t, marked, a.AppendRankFailure())

	// Follower: no publish after mark.
	a.RoleSet(RoleFollower)
	appendsBefore := len(eg.appends)
	require.Equal(t, 0, a.consensusWork(timeoutNs*4))
	require.Len(t, eg.appends, appendsBefore)
	require.Equal(t, marked, a.AppendRankFailure())
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

func TestFollowerConsensusWork_nilAdapter_appendOnly(t *testing.T) {
	// Nil LogAdapter: append publish only; commit not advanced via proposeMax.
	appendV := new(atomic.Int64)
	appendV.Store(40)
	eg := &recordingEgress{}
	commit := new(atomic.Int64)
	commit.Store(10)
	a := NewAgent(AgentConfig{
		Role:             RoleFollower,
		Egress:           eg,
		AppendSource:     &AtomicAppendPosition{V: appendV},
		Commit:           commit,
		LeadershipTermID: 3,
	})
	require.Nil(t, a.logAdapter)

	require.Equal(t, 1, a.consensusWork(0))
	require.Equal(t, []appendPublishCall{{term: 3, pos: 40, flags: 0}}, eg.appends)
	require.Equal(t, int64(10), a.CommitPosition())
}

// requireAppendPosition asserts a source reports ready with want.
func requireAppendPosition(t *testing.T, want int64, src AppendPositionSource) {
	t.Helper()
	got, ready := src.AppendPosition()
	require.True(t, ready)
	require.Equal(t, want, got)
}

// requireAppendNotReady asserts a source reports not-ready with position 0.
func requireAppendNotReady(t *testing.T, src AppendPositionSource) {
	t.Helper()
	got, ready := src.AppendPosition()
	require.False(t, ready)
	require.Equal(t, int64(0), got)
}

// countersWithRecordingPosition builds a counters reader holding at most one
// type-100 record. A caller may seed the counter, omit it, or leave a record for
// another recording id. Serves both *testing.T and *rapid.T callers.
func countersWithRecordingPosition(
	t interface {
		require.TestingT
		Helper()
	},
	recordingID int64,
	value int64,
	present bool,
) *cnc.CountersReader {
	t.Helper()
	// One slot when absent (unused scan stop). Two when present: allocated then
	// unused stop.
	counterCount := 1
	if present {
		counterCount = 2
	}
	meta := make([]byte, counterCount*cnc.CounterMetadataLength)
	values := make([]byte, counterCount*cnc.CounterValueLength)
	if present {
		require.NoError(t, cnc.WriteRecordingPositionCounterRecord(
			meta,
			values,
			0,
			cnc.CounterRecordAllocated,
			cnc.ArchiveRecordingPositionCounterTypeID,
			recordingID,
			1, // session
			"aeron:ipc",
			cnc.NullValue,
			value,
		))
	}
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)
	return reader
}

// steppingAppendSource counts reads and advances after each one, so a second
// read within one tick returns a different value and shows up in the publish.
type steppingAppendSource struct {
	value int64
	step  int64
	reads int
}

func (s *steppingAppendSource) AppendPosition() (int64, bool) {
	v := s.value
	s.reads++
	s.value += s.step
	return v, true
}

func TestFollowerConsensusWork_readsAppendSourceOncePerTick(t *testing.T) {
	eg := &recordingEgress{}
	src := &steppingAppendSource{value: 40, step: 7}
	a := NewAgent(AgentConfig{
		Role:             RoleFollower,
		Egress:           eg,
		AppendSource:     src,
		LeadershipTermID: 3,
	})
	a.SetLogAdapter(NewLogAdapter(0))

	// One read serves both the poll bound and the publish; the publish carries
	// that snapshot even though the source advanced after the read.
	require.Equal(t, 1, a.consensusWork(0))
	require.Equal(t, 1, src.reads)
	require.Equal(t, []appendPublishCall{{term: 3, pos: 40, flags: 0}}, eg.appends)

	// Next tick reads once more and publishes the next snapshot.
	require.Equal(t, 1, a.consensusWork(1))
	require.Equal(t, 2, src.reads)
	require.Equal(t, appendPublishCall{term: 3, pos: 47, flags: 0}, eg.appends[1])
}

func TestFollowerLogPollBound_nilAppendSource(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Nil(t, a.AppendSource())

	const notify int64 = 4242
	a.notifiedCommitPosition = notify
	// Nil append source: bound is notified only, appendPos ignored.
	require.Equal(t, notify, a.followerLogPollBound(0))
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
	requireAppendPosition(t, 0, (*AtomicAppendPosition)(nil))
	requireAppendPosition(t, 0, &AtomicAppendPosition{})
	v := new(atomic.Int64)
	v.Store(9)
	requireAppendPosition(t, 9, &AtomicAppendPosition{V: v})
}

// fakeLogRecordingControl is a scripted LogRecordingControl for agent tests.
// The fake records every call and returns the next scripted outcome.
//
// onStart runs after the start call is recorded and before the scripted
// outcome returns. Tests may close the join image mid-start from onStart.
//
// active is true after a successful start until a clean stop. That matches
// [LogRecordingControl.IsActive].
//
// nextStopPositionFor scripts StopPositionFor on success. Zero is a valid
// scripted position. Set nextStopPositionFor explicitly when prepare reaches
// derivation.
//
// stopSignalled and stopSignalPosition script one
// [LogRecordingControl.ConsumeStopSignal] hit.
//
// pollStopPending scripts [LogRecordingControl.PollStopLogRecording] as not
// done. stopErr fails both StopLogRecording and the poll. pollStopCalls
// counts PollStopLogRecording. stopCalls counts StopLogRecording only.
type fakeLogRecordingControl struct {
	startCalls           []fakeStartCall
	stopCalls            int
	pollStopCalls        int
	pollStopNowNs        int64
	pollStopPending      bool
	nextRecordingID      int64
	nextStopPositionFor  int64
	stopPositionForCalls []int64
	startErr             error
	stopErr              error
	stopPositionForErr   error
	onStart              func()
	active               bool
	stopSignalled        bool
	stopSignalPosition   int64
}

type fakeStartCall struct {
	sourceLocation   archivewire.SourceLocation
	logSessionID     int32
	recordingChannel string
}

var _ LogRecordingControl = (*fakeLogRecordingControl)(nil)

func (f *fakeLogRecordingControl) StartLogRecording(
	sourceLocation archivewire.SourceLocation,
	logSessionID int32,
	recordingChannel string,
) (int64, error) {
	f.startCalls = append(f.startCalls, fakeStartCall{
		sourceLocation:   sourceLocation,
		logSessionID:     logSessionID,
		recordingChannel: recordingChannel,
	})
	if f.onStart != nil {
		f.onStart()
	}
	if f.startErr != nil {
		return 0, f.startErr
	}
	f.active = true
	return f.nextRecordingID, nil
}

func (f *fakeLogRecordingControl) StopLogRecording() error {
	f.stopCalls++
	if f.stopErr != nil {
		return f.stopErr
	}
	f.active = false
	return nil
}

func (f *fakeLogRecordingControl) PollStopLogRecording(nowNs int64) (bool, error) {
	f.pollStopCalls++
	f.pollStopNowNs = nowNs
	if f.stopErr != nil {
		return false, f.stopErr
	}
	if f.pollStopPending {
		return false, nil
	}
	f.active = false
	return true, nil
}

func (f *fakeLogRecordingControl) IsActive() bool {
	return f.active
}

func (f *fakeLogRecordingControl) ConsumeStopSignal() (int64, bool) {
	if !f.stopSignalled {
		return recordinglog.NullPosition, false
	}
	f.stopSignalled = false
	return f.stopSignalPosition, true
}

func (f *fakeLogRecordingControl) StopPositionFor(recordingID int64) (int64, error) {
	f.stopPositionForCalls = append(f.stopPositionForCalls, recordingID)
	if f.stopPositionForErr != nil {
		return recordinglog.NullPosition, f.stopPositionForErr
	}
	return f.nextStopPositionFor, nil
}

func TestAgent_logRecording_nilControlLeaderJoin(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{})
	require.NoError(t, a.JoinLogAsLeader(1, 10, 2, true))
	require.False(t, a.IsRecordingActive())
	require.Equal(t, int64(nullValue), a.LocalLogRecordingID())
}

func TestAgent_logRecording_followerJoinNoImage(t *testing.T) {
	t.Parallel()

	// Image check precedes the start, so a session that vanished never starts.
	control := &fakeLogRecordingControl{nextRecordingID: 11}
	src := &stubLogJoinSource{}
	adapter := NewLogAdapter(0)
	a := NewAgent(AgentConfig{
		LogJoinSource: src,
		LogAdapter:    adapter,
	})
	a.SetLogRecordingControl(control)

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: 7}, 1)
	require.NoError(t, err)
	require.False(t, joined)
	require.Empty(t, control.startCalls)
	require.False(t, a.IsRecordingActive())
}

func TestAgent_logRecording_transitionNoRecording(t *testing.T) {
	t.Parallel()

	// Wired control always receives stop so a dirty start still drains. Absent
	// retained recording id: prepare returns logPosition. Does not return a
	// derived stop position.
	control := &fakeLogRecordingControl{nextStopPositionFor: 999}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)

	got, prepared, err := a.PrepareForNewLeadership(1234, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, int64(1234), got)
	require.Equal(t, 1, control.pollStopCalls)
	require.Equal(t, 0, control.stopCalls)
	require.Empty(t, control.stopPositionForCalls)
}

func TestAgent_logRecording_prepareDerivesFromRetainedID(t *testing.T) {
	t.Parallel()

	// Successful StopPositionFor for a retained id returns the derived
	// position. Does not return the caller logPosition.
	control := &fakeLogRecordingControl{nextStopPositionFor: 5000}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)
	a.localLogRecordingID = 77

	got, prepared, err := a.PrepareForNewLeadership(4096, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, int64(5000), got)
	require.NotEqual(t, int64(4096), got)
	require.Equal(t, 1, control.pollStopCalls)
	require.Equal(t, 0, control.stopCalls)
	require.Equal(t, []int64{77}, control.stopPositionForCalls)
}

func TestAgent_logRecording_prepareDerivationFailureDoesNotFallBack(t *testing.T) {
	t.Parallel()

	// Derivation error returns NullPosition. Does not fall back to logPosition.
	errBoom := errors.New("derive failed")
	control := &fakeLogRecordingControl{stopPositionForErr: errBoom}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)
	a.localLogRecordingID = 77

	got, prepared, err := a.PrepareForNewLeadership(4096, 1)
	require.ErrorIs(t, err, errBoom)
	require.False(t, prepared)
	require.False(t, a.prepareInProgress)
	require.Equal(t, recordinglog.NullPosition, got)
	require.NotEqual(t, int64(4096), got)
	require.Equal(t, 1, control.pollStopCalls)
	require.Equal(t, 0, control.stopCalls)
	require.Equal(t, []int64{77}, control.stopPositionForCalls)
}

func TestAgent_logRecording_prepareStopErrorWhileInactive(t *testing.T) {
	t.Parallel()

	// Asserts prepare still stops when inactive (dirty-start drain path).
	//
	// Stop error surfaces so election re-init retries the drain.
	stopErr := errors.New("dirty stop failed")
	control := &fakeLogRecordingControl{stopErr: stopErr}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)

	got, prepared, err := a.PrepareForNewLeadership(1234, 1)
	require.ErrorIs(t, err, stopErr)
	require.False(t, prepared)
	require.Equal(t, recordinglog.NullPosition, got)
	require.False(t, a.IsRecordingActive())
	require.Equal(t, 1, control.pollStopCalls)
	require.Equal(t, 0, control.stopCalls)
}

func TestAgent_logRecording_prepareNilControlKeepsRecordingID(t *testing.T) {
	t.Parallel()

	// Asserts prepare with nil control skips stop, stays inactive, and keeps the
	// seeded recording id.
	a := NewAgent(AgentConfig{})
	a.localLogRecordingID = 77
	require.Nil(t, a.LogRecordingControl())

	got, prepared, err := a.PrepareForNewLeadership(1234, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.False(t, a.prepareInProgress)
	require.Equal(t, int64(1234), got)
	require.False(t, a.IsRecordingActive())
	require.Equal(t, int64(77), a.LocalLogRecordingID())
}

func TestAgent_logRecording_setControlNilReportsInactive(t *testing.T) {
	t.Parallel()

	// Asserts SetLogRecordingControl(nil) makes IsRecordingActive false.
	//
	// The detached control may still report IsActive true. The agent has no
	// control to query.
	lp, _ := boundLogPublisher(t)
	control := &fakeLogRecordingControl{nextRecordingID: 1}
	a := NewAgent(AgentConfig{LogPublisher: lp})
	a.SetLogRecordingControl(control)
	require.NoError(t, a.JoinLogAsLeader(1, 10, 2, false))
	require.True(t, a.IsRecordingActive())
	require.True(t, control.IsActive())

	a.SetLogRecordingControl(nil)
	require.Nil(t, a.LogRecordingControl())
	require.False(t, a.IsRecordingActive())
	require.True(t, control.IsActive())
}

func TestAgent_logRecording_sentinelLogPositionNoRecording(t *testing.T) {
	t.Parallel()

	// Sentinel travels through the no-recording branch unchanged.
	a := NewAgent(AgentConfig{})
	got, prepared, err := a.PrepareForNewLeadership(recordinglog.NullPosition, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, recordinglog.NullPosition, got)
}

func TestAgent_logRecording_leaderJoinUnboundPublication(t *testing.T) {
	t.Parallel()

	control := &fakeLogRecordingControl{nextRecordingID: 77}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)

	err := a.JoinLogAsLeader(1, 10, 2, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "consensus: join log as leader: log publication is not bound")
	require.Empty(t, control.startCalls)
	require.False(t, a.IsRecordingActive())

	// Unbound LogPublisher (no publication) is the same structural refusal.
	a2 := NewAgent(AgentConfig{LogPublisher: NewLogPublisher()})
	a2.SetLogRecordingControl(control)
	err = a2.JoinLogAsLeader(1, 10, 2, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "consensus: join log as leader: log publication is not bound")
}

// Asserts the leader recording start checks the publication and never
// reseeds.
func TestAgent_logRecording_leaderJoinChecksPublication(t *testing.T) {
	t.Parallel()

	const (
		recordingID int64 = 7
		matchPos    int64 = 0
		otherPos    int64 = 100
	)

	tests := []struct {
		name         string
		noHook       bool
		recordingID  int64
		logPosition  int64
		otherSession bool
		wantErr      bool
	}{
		{
			name:        "position differs fails closed",
			recordingID: recordingID,
			logPosition: otherPos,
			wantErr:     true,
		},
		{
			name:         "session differs fails closed",
			recordingID:  recordingID,
			logPosition:  matchPos,
			otherSession: true,
			wantErr:      true,
		},
		{
			name:        "position and session match starts the recording",
			recordingID: recordingID,
			logPosition: matchPos,
		},
		{
			name:        "no hook skips the check",
			noHook:      true,
			recordingID: recordingID,
			logPosition: otherPos,
		},
		{
			name:        "negative recording id skips the check",
			recordingID: nullValue,
			logPosition: otherPos,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lp, _ := boundLogPublisher(t)
			control := &fakeLogRecordingControl{nextRecordingID: recordingID}
			a := NewAgent(AgentConfig{LogPublisher: lp})
			a.SetLogRecordingControl(control)
			a.SetLogRecordingID(tt.recordingID)
			if !tt.noHook {
				a.SetLogPublicationReseed(func(position int64) (int32, error) {
					t.Fatalf("unexpected reseed at %d", position)
					return 0, nil
				})
			}
			logSessionID := lp.SessionID()
			if tt.otherSession {
				logSessionID++
			}

			err := a.JoinLogAsLeader(1, tt.logPosition, logSessionID, false)
			if tt.wantErr {
				require.EqualError(t, err, fmt.Sprintf(
					"consensus: join log as leader: log publication %d at %d, election session %d at %d",
					lp.SessionID(), lp.Position(), logSessionID, tt.logPosition,
				))
				require.Empty(t, control.startCalls)
				require.False(t, a.IsRecordingActive())
				return
			}
			require.NoError(t, err)
			require.Len(t, control.startCalls, 1)
			require.Equal(t, archivewire.SourceLocation_LOCAL, control.startCalls[0].sourceLocation)
			require.Equal(t, logSessionID, control.startCalls[0].logSessionID)
			require.True(t, a.IsRecordingActive())
		})
	}
}

func TestAgent_SetLogRecordingID_whileInactive(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{})
	a.SetLogRecordingID(4242)
	require.Equal(t, int64(4242), a.LocalLogRecordingID())
	require.False(t, a.IsRecordingActive())
}

// TestOnSteadyCanvassPosition_agentMatrix asserts agent settled-canvass rows:
// role and term checks, echo, session sentinel, and adapter-refusal shape.
func TestOnSteadyCanvassPosition_agentMatrix(t *testing.T) {
	t.Parallel()

	const (
		leaderTerm   int64 = 5
		canvassPos   int64 = 100
		followerID   int32 = 1
		thisMemberID int32 = 0
		commitPos    int64 = 50
		recordingID  int64 = 77
		nowNs        int64 = 1_000
	)

	baseCanvass := codecs.CanvassPositionFields{
		LogLeadershipTermID: leaderTerm,
		LogPosition:         canvassPos,
		LeadershipTermID:    leaderTerm,
		FollowerMemberID:    followerID,
	}

	newLeaderFixture := func(
		role Role,
		term int64,
		lp *LogPublisher,
	) (*Agent, *spyPositionTable, *recordingEgress) {
		commit := new(atomic.Int64)
		commit.Store(commitPos)
		pt := &spyPositionTable{}
		eg := &recordingEgress{}
		a := NewAgent(AgentConfig{
			Role:             role,
			Commit:           commit,
			Positions:        pt,
			Egress:           eg,
			ThisMemberID:     thisMemberID,
			LeadershipTermID: term,
			LogPublisher:     lp,
		})
		a.localLogRecordingID = recordingID
		return a, pt, eg
	}

	drive := func(a *Agent, fields codecs.CanvassPositionFields) {
		wire := encodeCanvassPositionWire(t, fields)
		a.handleConsensusFragment(wire, 0, nowNs)
	}

	t.Run("election live routes to runner", func(t *testing.T) {
		t.Parallel()
		a, pt, eg := newLeaderFixture(RoleLeader, leaderTerm, nil)
		runner := &recordingRunner{}
		a.SetElection(runner)
		drive(a, baseCanvass)
		require.Empty(t, eg.answers)
		require.Empty(t, pt.updates)
		require.Len(t, runner.handles, 1)
	})

	t.Run("role follower refuses", func(t *testing.T) {
		t.Parallel()
		a, pt, eg := newLeaderFixture(RoleFollower, leaderTerm, nil)
		drive(a, baseCanvass)
		require.Empty(t, eg.answers)
		require.Empty(t, pt.updates)
	})

	t.Run("role candidate refuses", func(t *testing.T) {
		t.Parallel()
		a, pt, eg := newLeaderFixture(RoleCandidate, leaderTerm, nil)
		drive(a, baseCanvass)
		require.Empty(t, eg.answers)
		require.Empty(t, pt.updates)
	})

	t.Run("canvasser term above refuses without update", func(t *testing.T) {
		t.Parallel()
		a, pt, eg := newLeaderFixture(RoleLeader, leaderTerm, nil)
		fields := baseCanvass
		fields.LogLeadershipTermID = leaderTerm + 1
		drive(a, fields)
		require.Empty(t, eg.answers)
		require.Empty(t, pt.updates)
	})

	t.Run("canvasser term equal publishes", func(t *testing.T) {
		t.Parallel()
		a, pt, eg := newLeaderFixture(RoleLeader, leaderTerm, nil)
		drive(a, baseCanvass)
		require.Len(t, eg.answers, 1)
		require.Len(t, pt.updates, 1)
	})

	t.Run("unknown canvasser id still updates position table", func(t *testing.T) {
		t.Parallel()
		a, pt, eg := newLeaderFixture(RoleLeader, leaderTerm, nil)
		fields := baseCanvass
		fields.FollowerMemberID = 99
		drive(a, fields)
		require.Len(t, pt.updates, 1)
		require.Equal(t, memberPositionUpdate{
			memberID:         99,
			leadershipTermID: leaderTerm,
			logPosition:      canvassPos,
		}, pt.updates[0])
		require.Len(t, eg.answers, 1)
		require.Equal(t, int32(99), eg.answers[0].FollowerMemberID)
	})

	t.Run("echoes canvasser log leadership term", func(t *testing.T) {
		t.Parallel()
		const canvasserTerm int64 = 3
		a, pt, eg := newLeaderFixture(RoleLeader, leaderTerm, nil)
		fields := baseCanvass
		fields.LogLeadershipTermID = canvasserTerm
		drive(a, fields)
		require.Len(t, eg.answers, 1)
		require.Equal(t, canvasserTerm, eg.answers[0].LogLeadershipTermID)
		require.Equal(t, leaderTerm, eg.answers[0].LeadershipTermID)
		require.Len(t, pt.updates, 1)
		require.Equal(t, memberPositionUpdate{
			memberID:         followerID,
			leadershipTermID: canvasserTerm,
			logPosition:      canvassPos,
		}, pt.updates[0])
	})

	t.Run("bound publication fills position and session", func(t *testing.T) {
		t.Parallel()
		lp, _ := boundLogPublisher(t)
		a, _, eg := newLeaderFixture(RoleLeader, leaderTerm, lp)
		drive(a, baseCanvass)
		require.Len(t, eg.answers, 1)
		ans := eg.answers[0]
		require.Equal(t, lp.Position(), ans.LogPosition)
		require.Equal(t, lp.SessionID(), ans.LogSessionID)
		require.NotEqual(t, nullMemberID, ans.LogSessionID)
		require.Equal(t, commitPos, ans.CommitPosition)
		require.Equal(t, recordingID, ans.LeaderRecordingID)
	})

	t.Run("bound publication past zero answers without reseed", func(t *testing.T) {
		t.Parallel()
		lp, _ := boundLogPublisher(t)
		require.Positive(t, lp.AppendMessage(leaderTerm, 2, 3, []byte("entry")))
		a, _, eg := newLeaderFixture(RoleLeader, leaderTerm, lp)
		a.SetLogPublicationReseed(func(position int64) (int32, error) {
			t.Fatalf("unexpected reseed at %d", position)
			return 0, nil
		})
		drive(a, baseCanvass)
		require.Len(t, eg.answers, 1)
		require.Equal(t, lp.SessionID(), eg.answers[0].LogSessionID)
		require.Equal(t, lp.Position(), eg.answers[0].LogPosition)
	})

	t.Run("default local recording id is null", func(t *testing.T) {
		t.Parallel()
		commit := new(atomic.Int64)
		commit.Store(commitPos)
		eg := &recordingEgress{}
		a := NewAgent(AgentConfig{
			Role:             RoleLeader,
			Commit:           commit,
			Positions:        &spyPositionTable{},
			Egress:           eg,
			ThisMemberID:     thisMemberID,
			LeadershipTermID: leaderTerm,
		})
		// NewAgent leaves localLogRecordingID at nullValue. Do not seed.
		drive(a, baseCanvass)
		require.Len(t, eg.answers, 1)
		require.Equal(t, int64(nullValue), eg.answers[0].LeaderRecordingID)
	})

	t.Run("nil log publisher fills null session id", func(t *testing.T) {
		t.Parallel()
		a, _, eg := newLeaderFixture(RoleLeader, leaderTerm, nil)
		drive(a, baseCanvass)
		require.Len(t, eg.answers, 1)
		require.Equal(t, nullMemberID, eg.answers[0].LogSessionID)
		require.NotEqual(t, int32(0), eg.answers[0].LogSessionID)
		require.Equal(t, int64(0), eg.answers[0].LogPosition)
	})

	t.Run("unbound log publisher fills null session id", func(t *testing.T) {
		t.Parallel()
		a, _, eg := newLeaderFixture(RoleLeader, leaderTerm, NewLogPublisher())
		drive(a, baseCanvass)
		require.Len(t, eg.answers, 1)
		require.Equal(t, nullMemberID, eg.answers[0].LogSessionID)
		require.NotEqual(t, int32(0), eg.answers[0].LogSessionID)
	})

	t.Run("nil positions or egress does not panic", func(t *testing.T) {
		t.Parallel()
		// Both nil: refuse before either call; no spy to observe.
		require.NotPanics(t, func() {
			a := NewAgent(AgentConfig{
				Role:             RoleLeader,
				LeadershipTermID: leaderTerm,
			})
			drive(a, baseCanvass)
		})
		// Positions wired, egress nil: no panic; no publish surface.
		pt := &spyPositionTable{}
		require.NotPanics(t, func() {
			a := NewAgent(AgentConfig{
				Role:             RoleLeader,
				LeadershipTermID: leaderTerm,
				Positions:        pt,
			})
			drive(a, baseCanvass)
		})
		require.Empty(t, pt.updates)
		// Positions nil, egress wired: no panic and no PublishNewLeadershipTerm.
		eg := &recordingEgress{}
		require.NotPanics(t, func() {
			a := NewAgent(AgentConfig{
				Role:             RoleLeader,
				LeadershipTermID: leaderTerm,
				Egress:           eg,
			})
			drive(a, baseCanvass)
		})
		require.Empty(t, eg.answers)
	})

	t.Run("egress false leaves position and reports no error", func(t *testing.T) {
		t.Parallel()
		a, pt, eg := newLeaderFixture(RoleLeader, leaderTerm, nil)
		eg.refuseTerm = true
		drive(a, baseCanvass)
		require.Len(t, pt.updates, 1)
		require.Len(t, eg.answers, 1)
		// Agent tolerates false: no second publish, no panic, no rollback.
		require.Equal(t, memberPositionUpdate{
			memberID:         followerID,
			leadershipTermID: leaderTerm,
			logPosition:      canvassPos,
		}, pt.updates[0])
	})
}

func TestAgent_TruncateLogEntry_refusals(t *testing.T) {
	t.Parallel()

	const recordingID int64 = 77
	const alignedPosition int64 = 128

	tests := []struct {
		name     string
		setup    func(a *Agent, control *fakeLogTruncateControl)
		position int64
		wantErr  error
	}{
		{
			name: "no truncate control",
			setup: func(a *Agent, _ *fakeLogTruncateControl) {
				a.SetLogRecordingID(recordingID)
			},
			position: alignedPosition,
			wantErr:  errNoLogTruncateControl,
		},
		{
			name: "recording active",
			setup: func(a *Agent, control *fakeLogTruncateControl) {
				a.SetLogTruncateControl(control)
				a.SetLogRecordingID(recordingID)
				a.SetLogRecordingControl(&fakeLogRecordingControl{active: true})
			},
			position: alignedPosition,
			wantErr:  errTruncateWhileRecording,
		},
		{
			name: "null recording id",
			setup: func(a *Agent, control *fakeLogTruncateControl) {
				a.SetLogTruncateControl(control)
			},
			position: alignedPosition,
			wantErr:  errTruncateUnknownRecording,
		},
		{
			name: "negative position",
			setup: func(a *Agent, control *fakeLogTruncateControl) {
				a.SetLogTruncateControl(control)
				a.SetLogRecordingID(recordingID)
			},
			position: -32,
			wantErr:  errTruncatePositionMisaligned,
		},
		{
			name: "misaligned position",
			setup: func(a *Agent, control *fakeLogTruncateControl) {
				a.SetLogTruncateControl(control)
				a.SetLogRecordingID(recordingID)
			},
			position: 150,
			wantErr:  errTruncatePositionMisaligned,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			control := &fakeLogTruncateControl{}
			a := NewAgent(AgentConfig{})
			tc.setup(a, control)

			require.ErrorIs(t, a.TruncateLogEntry(0, tc.position), tc.wantErr)
			require.Empty(t, control.calls)
		})
	}
}

// TestAgent_TruncateLogEntry_successWithoutAdapter asserts a nil log adapter
// truncate calls the control and keeps the recording id. Archive-refuse cases
// live in log_truncate_testhooks_test.go beside the image-bound tests.
func TestAgent_TruncateLogEntry_successWithoutAdapter(t *testing.T) {
	t.Parallel()

	const recordingID int64 = 77
	const position int64 = 128

	control := &fakeLogTruncateControl{}
	a := NewAgent(AgentConfig{})
	a.SetLogTruncateControl(control)
	a.SetLogRecordingID(recordingID)

	require.NoError(t, a.TruncateLogEntry(0, position))
	require.Equal(t, []fakeTruncateCall{{
		recordingID: recordingID,
		position:    position,
	}}, control.calls)
	require.Equal(t, recordingID, a.LocalLogRecordingID())
}

// TestAgent_PrepareForNewLeadership_clearsAppendRankFailure asserts prepare
// always clears a marked append-rank failure and the not-ready run time so a
// leadership transition does not freeze ranking.
func TestAgent_PrepareForNewLeadership_clearsAppendRankFailure(t *testing.T) {
	t.Parallel()

	// Seed marked state. Mark path lives in resolveAppendPosition tests.
	a := NewAgent(AgentConfig{})
	a.appendRankFailure = ErrAppendPositionUnavailable
	a.appendNotReadySinceNs = 0

	_, prepared, err := a.PrepareForNewLeadership(1234, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.NoError(t, a.AppendRankFailure())
	require.Equal(t, int64(nullValue), a.appendNotReadySinceNs)
}

// TestAgent_PrepareForNewLeadership_derivesStopPosition asserts prepare stops
// the recording and adopts [LogRecordingControl.StopPositionFor] for the
// retained id. After a truncate the control reports the truncate position. The
// agent holds no stop position of its own.
func TestAgent_PrepareForNewLeadership_derivesStopPosition(t *testing.T) {
	t.Parallel()

	const (
		recordingID     int64 = 77
		derivedPosition int64 = 5000
	)

	control := &fakeLogRecordingControl{nextStopPositionFor: derivedPosition}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)
	a.SetLogRecordingID(recordingID)

	got, prepared, err := a.PrepareForNewLeadership(4096, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.False(t, a.prepareInProgress)
	require.Equal(t, derivedPosition, got)
	require.Equal(t, []int64{recordingID}, control.stopPositionForCalls)
	require.Equal(t, 1, control.pollStopCalls)
	require.Equal(t, 0, control.stopCalls)
}

type countingConsensusIngress struct {
	polls int
}

func (c *countingConsensusIngress) Poll(int, func([]byte, int)) int {
	c.polls++
	return 0
}

var _ ConsensusIngress = (*countingConsensusIngress)(nil)

// TestPrepareForNewLeadership_errorStartsFreshEpisode asserts a poll error
// ends the episode. The next call re-runs teardown.
func TestPrepareForNewLeadership_errorStartsFreshEpisode(t *testing.T) {
	t.Parallel()

	stopErr := errors.New("poll stop failed")
	control := &fakeLogRecordingControl{stopErr: stopErr}
	catchup := &fakeCatchupControl{}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)
	a.SetCatchupControl(catchup)
	a.lastPublishedAppendPosition = 50

	got, prepared, err := a.PrepareForNewLeadership(1234, 1)
	require.ErrorIs(t, err, stopErr)
	require.False(t, prepared)
	require.Equal(t, recordinglog.NullPosition, got)
	require.False(t, a.prepareInProgress)
	require.Equal(t, 1, catchup.stopCalls)

	control.stopErr = nil
	control.pollStopPending = true
	a.lastPublishedAppendPosition = 75
	got, prepared, err = a.PrepareForNewLeadership(1234, 2)
	require.NoError(t, err)
	require.False(t, prepared)
	require.Equal(t, recordinglog.NullPosition, got)
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)
	require.Equal(t, 2, catchup.stopCalls)
	require.True(t, a.prepareInProgress)
}

// TestPrepareForNewLeadership_skipsConsensusIngressWhilePending asserts
// DoWork skips the consensus ingress poll while an episode is open. The
// poll resumes after prepared or after a poll error.
func TestPrepareForNewLeadership_skipsConsensusIngressWhilePending(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stopErr error
	}{
		{name: "resumes after prepared"},
		{name: "resumes after error", stopErr: errors.New("poll stop failed")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			control := &fakeLogRecordingControl{pollStopPending: true}
			ingress := &countingConsensusIngress{}
			a := NewAgent(AgentConfig{Ingress: ingress})
			a.SetLogRecordingControl(control)

			_, prepared, err := a.PrepareForNewLeadership(1234, 1)
			require.NoError(t, err)
			require.False(t, prepared)
			require.True(t, a.prepareInProgress)
			require.Equal(t, 0, ingress.polls)

			a.DoWork(2)
			require.Equal(t, 0, ingress.polls)
			require.True(t, a.prepareInProgress)

			control.pollStopPending = false
			control.stopErr = tc.stopErr
			_, prepared, err = a.PrepareForNewLeadership(1234, 3)
			if tc.stopErr != nil {
				require.ErrorIs(t, err, tc.stopErr)
				require.False(t, prepared)
			} else {
				require.NoError(t, err)
				require.True(t, prepared)
			}
			require.False(t, a.prepareInProgress)

			a.DoWork(4)
			require.Equal(t, 1, ingress.polls)
		})
	}
}

// TestPrepareForNewLeadership_reinstallClearsEpisode asserts SetElection and
// installElection clear the episode flag. The next prepare re-runs teardown.
func TestPrepareForNewLeadership_reinstallClearsEpisode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		reinstall func(*Agent)
	}{
		{
			name:      "SetElection",
			reinstall: func(a *Agent) { a.SetElection(&orderRunner{n: 1}) },
		},
		{
			name: "installElection",
			reinstall: func(a *Agent) {
				a.installElection(&orderRunner{n: 1}, ElectionTriggerLeaderHeartbeatTimeout)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			control := &fakeLogRecordingControl{pollStopPending: true}
			catchup := &fakeCatchupControl{}
			a := NewAgent(AgentConfig{})
			a.SetLogRecordingControl(control)
			a.SetCatchupControl(catchup)

			_, prepared, err := a.PrepareForNewLeadership(1234, 1)
			require.NoError(t, err)
			require.False(t, prepared)
			require.True(t, a.prepareInProgress)
			require.Equal(t, 1, catchup.stopCalls)

			tc.reinstall(a)
			require.False(t, a.prepareInProgress)

			a.lastPublishedAppendPosition = 50
			_, prepared, err = a.PrepareForNewLeadership(1234, 2)
			require.NoError(t, err)
			require.False(t, prepared)
			require.Equal(t, int64(0), a.lastPublishedAppendPosition)
			require.Equal(t, 2, catchup.stopCalls)
		})
	}
}

type prepareDutyRunner struct {
	agent        *Agent
	logPosition  int64
	calls        int
	handles      int
	lastPrepared bool
	lastErr      error
}

func (r *prepareDutyRunner) DoWork(nowNs int64) int {
	r.calls++
	_, prepared, err := r.agent.PrepareForNewLeadership(r.logPosition, nowNs)
	r.lastPrepared = prepared
	r.lastErr = err
	return 1
}

func (r *prepareDutyRunner) HandleConsensusMessage([]byte, int) bool {
	r.handles++
	return true
}

func (r *prepareDutyRunner) HandleReplayNewLeadershipTermEvent(
	codecs.NewLeadershipTermEventFields,
) error {
	return nil
}

// TestPrepareForNewLeadership_dutyCycleIngressAndElection asserts Agent.DoWork
// with a live runner. Pending ticks skip consensus ingress and still run
// election.DoWork. A stop error ends the episode. The next tick polls ingress
// once (retry INIT drop) and re-calls prepare.
func TestPrepareForNewLeadership_dutyCycleIngressAndElection(t *testing.T) {
	t.Parallel()

	frame := encodeAppendPositionWire(t, codecs.AppendPositionFields{
		LeadershipTermID: 1,
		LogPosition:      50,
		FollowerMemberID: 2,
	})

	t.Run("pending then error polls ingress on retry tick", func(t *testing.T) {
		t.Parallel()

		control := &fakeLogRecordingControl{pollStopPending: true}
		q := &QueueConsensusIngress{}
		a := NewAgent(AgentConfig{Ingress: q})
		a.SetLogRecordingControl(control)
		runner := &prepareDutyRunner{agent: a, logPosition: 1234}
		a.SetElection(runner)

		// First tick starts the episode after the ingress poll.
		q.Enqueue(frame)
		require.Equal(t, 2, a.DoWork(1))
		require.True(t, a.prepareInProgress)
		require.False(t, runner.lastPrepared)
		require.NoError(t, runner.lastErr)
		require.Equal(t, 1, runner.calls)
		require.Equal(t, 1, runner.handles)
		require.Equal(t, 1, control.pollStopCalls)
		require.Equal(t, 0, control.stopCalls)

		q.Enqueue(frame)
		require.Equal(t, 1, a.DoWork(2))
		require.True(t, a.prepareInProgress)
		require.Equal(t, 2, runner.calls)
		require.Equal(t, 1, runner.handles)
		require.Equal(t, 2, control.pollStopCalls)
		require.Equal(t, 0, control.stopCalls)

		stopErr := errors.New("poll stop failed")
		control.pollStopPending = false
		control.stopErr = stopErr
		require.Equal(t, 1, a.DoWork(3))
		require.ErrorIs(t, runner.lastErr, stopErr)
		require.False(t, runner.lastPrepared)
		require.False(t, a.prepareInProgress)
		require.Equal(t, 3, runner.calls)
		require.Equal(t, 1, runner.handles)

		// Retry INIT tick polls once and starts a fresh episode.
		control.stopErr = nil
		control.pollStopPending = true
		require.Equal(t, 2, a.DoWork(4))
		require.True(t, a.prepareInProgress)
		require.False(t, runner.lastPrepared)
		require.NoError(t, runner.lastErr)
		require.Equal(t, 4, runner.calls)
		require.Equal(t, 2, runner.handles)
		require.Equal(t, 4, control.pollStopCalls)
		require.Equal(t, 0, control.stopCalls)
	})

	t.Run("pending then prepared resumes ingress next tick", func(t *testing.T) {
		t.Parallel()

		control := &fakeLogRecordingControl{pollStopPending: true}
		q := &QueueConsensusIngress{}
		a := NewAgent(AgentConfig{Ingress: q})
		a.SetLogRecordingControl(control)
		runner := &prepareDutyRunner{agent: a, logPosition: 1234}
		a.SetElection(runner)

		require.Equal(t, 1, a.DoWork(1))
		require.True(t, a.prepareInProgress)
		require.Equal(t, 1, runner.calls)
		require.Equal(t, 0, runner.handles)

		q.Enqueue(frame)
		require.Equal(t, 1, a.DoWork(2))
		require.True(t, a.prepareInProgress)
		require.Equal(t, 0, runner.handles)

		control.pollStopPending = false
		require.Equal(t, 1, a.DoWork(3))
		require.True(t, runner.lastPrepared)
		require.NoError(t, runner.lastErr)
		require.False(t, a.prepareInProgress)
		require.Equal(t, 0, runner.handles)

		require.Equal(t, 2, a.DoWork(4))
		require.Equal(t, 1, runner.handles)
	})
}

// TestPrepareForNewLeadership_derivationErrorStartsFreshEpisode asserts a
// StopPositionFor failure ends the episode. The next prepare re-runs
// teardown and hits StopPositionFor again.
func TestPrepareForNewLeadership_derivationErrorStartsFreshEpisode(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("derive failed")
	control := &fakeLogRecordingControl{stopPositionForErr: errBoom}
	catchup := &fakeCatchupControl{}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)
	a.SetCatchupControl(catchup)
	a.localLogRecordingID = 77
	a.lastPublishedAppendPosition = 50

	got, prepared, err := a.PrepareForNewLeadership(4096, 1)
	require.ErrorIs(t, err, errBoom)
	require.False(t, prepared)
	require.False(t, a.prepareInProgress)
	require.Equal(t, recordinglog.NullPosition, got)
	require.Equal(t, 1, catchup.stopCalls)
	require.Equal(t, []int64{77}, control.stopPositionForCalls)

	a.lastPublishedAppendPosition = 75
	got, prepared, err = a.PrepareForNewLeadership(4096, 2)
	require.ErrorIs(t, err, errBoom)
	require.False(t, prepared)
	require.False(t, a.prepareInProgress)
	require.Equal(t, recordinglog.NullPosition, got)
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)
	require.Equal(t, 2, catchup.stopCalls)
	require.Equal(t, []int64{77, 77}, control.stopPositionForCalls)
}

// TestPrepareForNewLeadership_pendingThenDoneDerivesStop asserts two pending
// ticks run teardown once. Later pending calls poll only.
// lastPublishedAppendPosition stays 0 after teardown. The done tick returns
// the derived stop. The done tick does not re-run teardown.
func TestPrepareForNewLeadership_pendingThenDoneDerivesStop(t *testing.T) {
	t.Parallel()

	const (
		recordingID     int64 = 77
		derivedPosition int64 = 5000
	)

	control := &fakeLogRecordingControl{
		pollStopPending:     true,
		nextStopPositionFor: derivedPosition,
	}
	catchup := &fakeCatchupControl{}
	a := NewAgent(AgentConfig{})
	a.SetLogRecordingControl(control)
	a.SetCatchupControl(catchup)
	a.SetLogRecordingID(recordingID)
	a.lastPublishedAppendPosition = 50
	a.timeOfLastAppendPositionSendNs = 100

	got, prepared, err := a.PrepareForNewLeadership(4096, 10)
	require.NoError(t, err)
	require.False(t, prepared)
	require.True(t, a.prepareInProgress)
	require.Equal(t, recordinglog.NullPosition, got)
	require.Equal(t, 1, catchup.stopCalls)
	require.Equal(t, 1, catchup.removeCalls)
	require.Equal(t, 1, control.pollStopCalls)
	require.Equal(t, 0, control.stopCalls)
	require.Equal(t, int64(10), control.pollStopNowNs)
	require.Equal(t, int64(0), a.lastPublishedAppendPosition)
	require.Equal(t, int64(0), a.timeOfLastAppendPositionSendNs)
	require.Empty(t, control.stopPositionForCalls)

	a.lastPublishedAppendPosition = 99
	got, prepared, err = a.PrepareForNewLeadership(4096, 20)
	require.NoError(t, err)
	require.False(t, prepared)
	require.True(t, a.prepareInProgress)
	require.Equal(t, recordinglog.NullPosition, got)
	require.Equal(t, 1, catchup.stopCalls)
	require.Equal(t, 1, catchup.removeCalls)
	require.Equal(t, 2, control.pollStopCalls)
	require.Equal(t, 0, control.stopCalls)
	require.Equal(t, int64(20), control.pollStopNowNs)
	require.Equal(t, int64(99), a.lastPublishedAppendPosition)
	require.Empty(t, control.stopPositionForCalls)

	control.pollStopPending = false
	got, prepared, err = a.PrepareForNewLeadership(4096, 30)
	require.NoError(t, err)
	require.True(t, prepared)
	require.False(t, a.prepareInProgress)
	require.Equal(t, derivedPosition, got)
	require.Equal(t, 1, catchup.stopCalls)
	require.Equal(t, 1, catchup.removeCalls)
	require.Equal(t, 3, control.pollStopCalls)
	require.Equal(t, 0, control.stopCalls)
	require.Equal(t, []int64{recordingID}, control.stopPositionForCalls)
}

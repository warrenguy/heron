package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// Host newLogReplication inject installs once and DoWork runs. Second lagging
// tick keeps the helper (no reinstall).
func TestFollowerLogReplication_installsWhenFactoryNonNil(t *testing.T) {
	t.Parallel()

	const leaderRecordingID int64 = 88
	const stopAhead int64 = 50

	cfg, deps, clock, _, _ := testElectionDeps(t)
	lr := &countingLogReplicationForTest{work: 3, recordingID: 77}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			return lr
		},
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)

	leader := election.MemberForTest(e, 1)
	require.NotNil(t, leader)
	leader.ArchiveEndpoint = "aeron:udp?endpoint=leader-archive"
	leader.ArchiveResponseEndpoint = "aeron:udp?endpoint=leader-archive-resp"

	election.SetStateForTest(e, election.ElectionStateCanvass)
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.LeaderRecordingID = leaderRecordingID
	fields.TermBaseLogPosition = cfg.AppendPosition + 100
	fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
	fields.NextTermBaseLogPosition = cfg.AppendPosition + stopAhead
	fields.NextLogPosition = election.NullPosition
	e.OnNewLeadershipTerm(fields)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

	appendBefore := e.AppendPosition()
	t0 := clock.nowNs

	// install (1) + DoWork (3)
	require.Equal(t, 4, e.DoWork(t0))
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
	require.Equal(t, 1, lr.doWorkCalls)
	require.Equal(t, appendBefore, e.AppendPosition())
	require.Len(t, host.newLogReplicationCalls, 1)
	call := host.newLogReplicationCalls[0]
	require.Equal(t, leader.ArchiveEndpoint, call.leaderArchiveEndpoint)
	require.Equal(t, leader.ArchiveResponseEndpoint, call.responseArchiveEndpoint)
	require.Equal(t, leaderRecordingID, call.leaderRecordingID)
	require.Equal(t, cfg.AppendPosition+stopAhead, call.stopPosition)
	require.Equal(t, t0, call.nowNs)

	// Still lagging: keep the helper; no second install.
	require.Equal(t, 3, e.DoWork(t0+1))
	require.Equal(t, 2, lr.doWorkCalls)
	require.Len(t, host.newLogReplicationCalls, 1)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
	require.Equal(t, appendBefore, e.AppendPosition())
}

// IsDone idles the helper without advancing append.
//
// Resident done helper blocks reinstall thrash and re-publish.
func TestFollowerLogReplication_IsDoneIdlesWithoutAppendAdvance(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	lr := &countingLogReplicationForTest{work: 1, done: true, recordingID: 91}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			return lr
		},
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

	appendBefore := e.AppendPosition()
	t0 := clock.nowNs

	// install (1) + DoWork (1) + IsDone publish (1)
	require.Equal(t, 3, e.DoWork(t0))
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
	require.Equal(t, appendBefore, e.AppendPosition())
	require.Equal(t, []int64{91}, host.setLogRecordingIDCalls)
	require.Equal(t, 1, lr.doWorkCalls)

	// Resident done helper: no reinstall thrash, no duty cycle, no re-publish
	// while still lagging.
	require.Equal(t, 0, e.DoWork(t0+1))
	require.Len(t, host.newLogReplicationCalls, 1)
	require.Equal(t, 1, lr.doWorkCalls)
	require.Equal(t, []int64{91}, host.setLogRecordingIDCalls)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
	require.Equal(t, appendBefore, e.AppendPosition())
}

// Nil host factory: re-call while lagging.
//
// Timeout and caught-up paths still hold.
func TestFollowerLogReplication_nilFactoryTimeoutCaughtUpOnly(t *testing.T) {
	t.Parallel()

	t.Run("nil factory times out while lagging", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		e := election.NewElection(cfg, deps)
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

		t0 := clock.nowNs
		require.Equal(t, 0, e.DoWork(t0))
		require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
		require.Len(t, host.newLogReplicationCalls, 1)
		require.Nil(t, host.newLogReplication)

		// Nil factory re-calls each lagging tick (not closed).
		require.Equal(t, 0, e.DoWork(t0+1))
		require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
		require.GreaterOrEqual(t, len(host.newLogReplicationCalls), 2)

		require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Empty(t, host.setLogRecordingIDCalls)
	})

	t.Run("nil factory caught up still canvasses", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, epochClock, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		spy := &recordingLogCoherencySpy{}
		deps.RecordingLog = spy
		e := election.NewElection(cfg, deps)

		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition)

		require.Equal(t, 0, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.Equal(t, cfg.LeadershipTermID+1, election.LogLeadershipTermIDForTest(e))
		require.Equal(t, []recordinglog.EnsureCoherentParams{{
			RecordingID:                42,
			InitialLogLeadershipTermID: cfg.LeadershipTermID,
			InitialTermBaseLogPosition: cfg.TermBaseLogPosition,
			LeadershipTermID:           cfg.LeadershipTermID + 1,
			TermBaseLogPosition:        cfg.AppendPosition,
			LogPosition:                cfg.AppendPosition,
			Timestamp:                  epochClock.nowMs,
		}}, spy.calls)
	})
}

// Not near-live without a leader: sticky INIT, near-live re-checked each tick,
// work 0. Stall ticks never call catchup start on the host.
func TestFollowerCatchupInit_notNearLiveSticky(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchupInit)

	t0 := clock.nowNs
	for i := range 3 {
		require.Equal(t, 0, e.DoWork(t0+int64(i)))
	}
	require.Equal(t, election.ElectionStateFollowerCatchupInit, e.State())
	require.Empty(t, host.catchupInitiatedCalls)
	require.Equal(t, []int64{0, 0, 0}, host.catchupNearLiveCalls)
}

// Catchup INIT with leader and catchup endpoint: send CatchupPosition, open
// destination, initiate, advance to AWAIT.
func TestFollowerCatchupInit_sendsCatchupPosition(t *testing.T) {
	t.Parallel()

	const (
		catchupEndpoint = "127.0.0.1:9005"
		catchupDest     = "aeron:udp?endpoint=" + catchupEndpoint
	)

	// Log equals append so install routes through no-replay to catchup init.
	fx := newElectionFixture(
		t,
		withPositions(testLeadershipTermID, testAppendPosition, testAppendPosition),
	)
	fx.host.ensureCatchupLogDestination = catchupDest
	e := fx.election

	driveToFollowerCatchupInit(fx, catchupEndpoint)

	t0 := fx.clock.nowNs + 1
	require.Equal(t, 1, e.DoWork(t0))
	require.Equal(t, election.ElectionStateFollowerCatchupAwait, e.State())
	require.Equal(t, []int64{t0}, fx.host.catchupInitiatedCalls)
	require.Equal(t, catchupDest, fx.host.CatchupLogDestination())

	require.Len(t, fx.publisher.Sent, 1)
	msg := fx.publisher.Sent[0]
	require.Equal(t, uint16(clusterwire.CatchupPositionSbeTemplateID), msg.TemplateID)
	cp := msg.Fields.(codecs.CatchupPositionFields)
	require.Equal(t, e.LeadershipTermID(), cp.LeadershipTermID)
	require.Equal(t, e.LogPosition(), cp.LogPosition)
	require.Equal(t, fx.cfg.MemberID, cp.FollowerMemberID)
	require.Equal(t, catchupEndpoint, cp.CatchupEndpoint)
	require.Equal(t, int32(1), msg.Dst.MemberID)
}

// Catchup destination open failure: sticky INIT, work 0 per tick, no
// CatchupPosition send. Past the leader heartbeat deadline the election
// restarts at INIT.
func TestFollowerCatchupInit_destinationOpenFailureIdles(t *testing.T) {
	t.Parallel()

	const catchupEndpoint = "127.0.0.1:9006"

	// Empty ensureCatchupLogDestination fails every open attempt.
	fx := newElectionFixture(
		t,
		withPositions(testLeadershipTermID, testAppendPosition, testAppendPosition),
	)
	driveToFollowerCatchupInit(fx, catchupEndpoint)
	e := fx.election

	t0 := fx.clock.nowNs + 1
	for i := range 3 {
		require.Equal(t, 0, e.DoWork(t0+int64(i)))
		require.Equal(t, election.ElectionStateFollowerCatchupInit, e.State())
	}
	require.Empty(t, fx.host.catchupInitiatedCalls)

	require.Equal(t, 1, e.DoWork(t0+fx.cfg.LeaderHeartbeatTimeoutNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Empty(t, fx.publisher.Sent)
}

// CatchupAwait joins when host reports ready at log position, then CATCHUP.
func TestFollowerCatchupAwait_joinsThenCatchup(t *testing.T) {
	t.Parallel()

	const logSessionID int32 = 7

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{
		clock:                   clock,
		followerLogJoinOK:       true,
		followerLogJoinPosition: cfg.LogPosition,
	}
	deps.Host = host
	handler := &recordingAgentHandler{joinFollowerReturn: true}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchupAwait)
	// logSessionID must be set for join ready to be meaningful.
	election.SetLogSessionIDForTest(e, logSessionID)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerCatchup, e.State())
	require.Equal(t, []int32{logSessionID}, host.followerLogJoinReadyCalls)
	require.Equal(t, 1, handler.joinFollowerCalls)
}

// CatchupAwait waits while join position mismatches (live late-join image).
func TestFollowerCatchupAwait_joinPositionMismatchWaits(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{
		clock:                   clock,
		followerLogJoinOK:       true,
		followerLogJoinPosition: cfg.LogPosition + 50,
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchupAwait)
	election.SetLogSessionIDForTest(e, 7)

	require.Equal(t, 0, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerCatchupAwait, e.State())
}

// CatchupAwait with no image: multi-tick work 0, sticky state.
func TestFollowerCatchupAwait_noImageReturnsZero(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchupAwait)

	t0 := clock.nowNs
	for i := range 3 {
		require.Equal(t, 0, e.DoWork(t0+int64(i)))
		require.Equal(t, election.ElectionStateFollowerCatchupAwait, e.State())
	}
	require.Empty(t, host.catchupPollCalls)
	require.Empty(t, host.catchupInitiatedCalls)
}

// Catchup poll reaches join bound: advances to LOG_INIT then READY when
// joined.
func TestFollowerCatchup_completesToReady(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	joinPos := cfg.AppendPosition + 100
	host := &recordingElectionHost{
		clock:             clock,
		catchupPollResult: 3,
		commitPosition:    joinPos,
		logJoined:         true,
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)

	election.SetStateForTest(e, election.ElectionStateCanvass)
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.LogPosition = joinPos
	fields.CommitPosition = joinPos
	e.OnNewLeadershipTerm(fields)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchup)

	// Poll work plus complete transition.
	require.Equal(t, 4, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerLogInit, e.State())
	require.Equal(t, joinPos, e.LogPosition())
	require.Equal(t, joinPos, e.AppendPosition())

	// Completion leaves the catchup receive path open. The adapter still reads
	// the catchup image, and prepare on the next Init closes the path.

	// Already joined: LOG_INIT to READY.
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateFollowerReady, e.State())
}

package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// The host newLogReplication factory builds the helper once. Later lagging
// ticks reuse the helper.
func TestFollowerLogReplication_installsWhenFactoryNonNil(t *testing.T) {
	t.Parallel()

	const leaderRecordingID int64 = 88

	cfg, deps, clock, _, _ := testElectionDeps(t)
	lr := &countingLogReplicationForTest{work: 3, recordingID: 77, position: -1}
	host := hostWithLogReplication(clock, lr)
	deps.Host = host
	e := election.NewElection(cfg, deps)

	leader := election.MemberForTest(e, 1)
	require.NotNil(t, leader)
	leader.ArchiveEndpoint = "aeron:udp?endpoint=leader-archive"
	leader.ArchiveResponseEndpoint = "aeron:udp?endpoint=leader-archive-resp"

	election.SetStateForTest(e, election.ElectionStateCanvass)
	fields := followerLogReplicationFields(
		cfg,
		cfg.AppendPosition+replicationStopAhead,
		election.NullPosition,
	)
	fields.LeaderRecordingID = leaderRecordingID
	e.OnNewLeadershipTerm(fields)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

	appendBefore := e.AppendPosition()
	t0 := clock.nowNs

	// The build tick does not run the helper.
	require.Equal(t, 1, e.DoWork(t0))
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
	require.Zero(t, lr.doWorkCalls)
	require.Len(t, host.newLogReplicationCalls, 1)
	call := host.newLogReplicationCalls[0]
	require.Equal(t, leader.ArchiveEndpoint, call.leaderArchiveEndpoint)
	require.Equal(t, leader.ArchiveResponseEndpoint, call.responseArchiveEndpoint)
	require.Equal(t, leaderRecordingID, call.leaderRecordingID)
	require.Equal(t, cfg.AppendPosition+replicationStopAhead, call.stopPosition)
	require.Equal(t, t0, call.nowNs)

	// Still lagging: the election reuses the helper. Position -1 sits below the
	// append position, so the election does not send AppendPosition.
	require.Equal(t, 3, e.DoWork(t0+1))
	require.Equal(t, 1, lr.doWorkCalls)
	require.Equal(t, 3, e.DoWork(t0+2))
	require.Equal(t, 2, lr.doWorkCalls)
	require.Len(t, host.newLogReplicationCalls, 1)
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

// Asserts catchup poll reaches join bound with catchup detached and live
// attached. Advances to LOG_INIT then READY when joined.
func TestFollowerCatchup_completesToReady(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	joinPos := cfg.AppendPosition + 100
	host := &recordingElectionHost{
		clock:             clock,
		catchupPollResult: 3,
		commitPosition:    joinPos,
		logJoined:         true,
		// Gate: catchup detached, live attached.
		catchupLogDestination: "",
		liveLogDestination:    "aeron:udp?endpoint=live-log",

		// Zero followerLogJoinPosition. Bound-session re-join does not compare
		// the join position against the election position.
		followerLogJoinOK: true,
	}
	deps.Host = host
	handler := &recordingAgentHandler{joinFollowerReturn: true}
	deps.Handler = handler
	e, _ := forceFollowerCatchup(t, cfg, deps, joinPos, 100)

	// Poll work plus complete transition.
	require.Equal(t, 4, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerLogInit, e.State())
	require.Equal(t, joinPos, e.LogPosition())
	require.Equal(t, joinPos, e.AppendPosition())

	// Already joined: LOG_INIT re-joins the bound session, then READY.
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateFollowerReady, e.State())
	require.Equal(t, 1, handler.joinFollowerCalls)
	require.True(t, handler.joinFollowerParams[0].IsBoundRejoin)
}

// Asserts OnStopCatchup with matching term and member removes the catchup
// destination.
func TestOnStopCatchup_removesCatchupDestination(t *testing.T) {
	t.Parallel()

	const catchupDest = "aeron:udp?endpoint=catchup"

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{
		clock:                       clock,
		ensureCatchupLogDestination: catchupDest,
		catchupLogDestination:       catchupDest,
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchup)

	match := stopCatchupFields(e.LeadershipTermID(), cfg.MemberID)
	e.OnStopCatchup(match)
	require.Equal(t, 1, host.removeCatchupLogDestinationCalls)
	require.Empty(t, host.CatchupLogDestination())

	// Idempotent second call.
	e.OnStopCatchup(match)
	require.Equal(t, 2, host.removeCatchupLogDestinationCalls)

	// Wrong member: no remove.
	e.OnStopCatchup(stopCatchupFields(e.LeadershipTermID(), cfg.MemberID+1))
	require.Equal(t, 2, host.removeCatchupLogDestinationCalls)

	// Wrong term: no remove.
	e.OnStopCatchup(stopCatchupFields(e.LeadershipTermID()+1, cfg.MemberID))
	require.Equal(t, 2, host.removeCatchupLogDestinationCalls)
}

// Asserts HandleConsensusMessage dispatches StopCatchup to OnStopCatchup.
func TestHandleConsensusMessage_stopCatchup(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{
		clock:                 clock,
		catchupLogDestination: "aeron:udp?endpoint=catchup",
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchup)

	fields := stopCatchupFields(e.LeadershipTermID(), cfg.MemberID)
	wireLen, ok := codecs.ClusterWireMessageLen(clusterwire.StopCatchupSbeBlockLength)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeStopCatchup(
		buf, 0, clusterwire.StopCatchupSbeSchemaVersion, fields,
	)
	require.True(t, ok)

	require.True(t, e.HandleConsensusMessage(buf[:end], 0))
	require.Equal(t, 1, host.removeCatchupLogDestinationCalls)
	require.Empty(t, host.CatchupLogDestination())
}

// Asserts catchup-flagged OnAppendPosition updates the member position.
// OnAppendPosition passes the wire log position to TrackCatchupCompletion.
func TestOnAppendPosition_catchupFlag_updatesPosition(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock, role: consensus.RoleLeader}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	follower := election.MemberForTest(e, 1)
	beforeStamp := follower.TimeOfLastAppendPositionNs

	const logPos int64 = 999
	e.OnAppendPosition(codecs.AppendPositionFields{
		LeadershipTermID: testLeadershipTermID,
		LogPosition:      logPos,
		FollowerMemberID: 1,
		Flags:            consensus.AppendPositionFlagCatchup,
	})

	require.Equal(t, logPos, follower.LogPosition)
	require.Equal(t, int64(testLeadershipTermID), follower.LeadershipTermID)
	require.Equal(t, clock.nowNs, follower.TimeOfLastAppendPositionNs)
	require.NotEqual(t, beforeStamp, follower.TimeOfLastAppendPositionNs)
	require.Len(t, host.trackCatchupCalls, 1)
	require.Equal(t, logPos, host.trackCatchupCalls[0].logPosition)
	require.Equal(t, consensus.AppendPositionFlagCatchup, host.trackCatchupCalls[0].flags)
}

// Asserts position at join without destination gate stays in CATCHUP.
//
// Phases:
//   - live empty, catchup set: blocked
//   - live attached, catchup still set: blocked
//   - OnStopCatchup clears catchup: gate passes
func TestFollowerCatchup_destinationGateBlocksComplete(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	joinPos := cfg.AppendPosition + 100
	host := &recordingElectionHost{
		clock:                 clock,
		catchupPollResult:     1,
		commitPosition:        joinPos,
		catchupLogDestination: "aeron:udp?endpoint=catchup",
		liveLogDestination:    "",
	}
	deps.Host = host
	e, _ := forceFollowerCatchup(t, cfg, deps, joinPos, 100)

	// Live empty, catchup set: blocked.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerCatchup, e.State())

	// Live attached, catchup still set: still blocked.
	host.liveLogDestination = "aeron:udp?endpoint=live-log"
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateFollowerCatchup, e.State())
	require.NotEmpty(t, host.CatchupLogDestination())
	require.NotEmpty(t, host.LiveLogDestination())

	// Leader StopCatchup detaches catchup. Gate passes on the next tick.
	e.OnStopCatchup(stopCatchupFields(e.LeadershipTermID(), cfg.MemberID))
	require.Equal(t, 1, host.removeCatchupLogDestinationCalls)
	require.Empty(t, host.CatchupLogDestination())

	require.Equal(t, 2, e.DoWork(clock.nowNs+2))
	require.Equal(t, election.ElectionStateFollowerLogInit, e.State())
}

func stopCatchupFields(leadershipTermID int64, followerMemberID int32) codecs.StopCatchupFields {
	return codecs.StopCatchupFields{
		LeadershipTermID: leadershipTermID,
		FollowerMemberID: followerMemberID,
	}
}

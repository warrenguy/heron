package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// leaderLogReplicationFixture drives a 3-node election to LEADER_LOG_REPLICATION
// with quorumPosition holding below the append position so the state does not
// advance.
func leaderLogReplicationFixture(
	t *testing.T,
	quorumPosition int64,
	opts ...fixtureOption,
) *electionFixture {
	t.Helper()

	fx := newElectionFixture(t, opts...)
	fx.host.quorumPosition = quorumPosition
	fx.canvassTo(election.ElectionStateCandidateBallot)
	fx.injectVote(1, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)
	fx.injectVote(2, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)
	fx.doWork()
	require.Equal(t, election.ElectionStateLeaderLogReplication, fx.election.State())
	fx.publisher.Reset()
	return fx
}

func TestHandleLeaderLogReplication_publishesOnLeaderHeartbeatInterval(t *testing.T) {
	t.Parallel()

	fx := leaderLogReplicationFixture(t, 0)

	// First cycle publishes NewLeadershipTerm and the commit position.
	require.Equal(t, 2, fx.doWork())
	require.Len(t, fx.publisher.Sent, 2)

	// Past the election status interval but inside the leader heartbeat
	// interval: no republish.
	fx.advance(fx.cfg.ElectionStatusIntervalNs)
	require.Equal(t, 0, fx.doWork())
	require.Len(t, fx.publisher.Sent, 2)

	fx.advance(fx.cfg.LeaderHeartbeatIntervalNs - fx.cfg.ElectionStatusIntervalNs)
	require.Equal(t, 2, fx.doWork())
	require.Len(t, fx.publisher.Sent, 4)
	for _, m := range fx.publisher.Sent {
		require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), m.TemplateID)
	}
}

func TestBecomeSelfLeader_resetsOnLogPublicationError(t *testing.T) {
	t.Parallel()

	cause := errors.New("log publication add failed")
	observer := &recordingStateObserver{}
	fx := newElectionFixture(t, withAddLogPublicationErr(cause), withStateObserver(observer))
	appendPosition := fx.cfg.AppendPosition

	// Phase 1: the win resets in the same duty cycle with the add error.
	fx.canvassTo(election.ElectionStateCandidateBallot)
	fx.injectVote(1, fx.cfg.LeadershipTermID, appendPosition, true)
	fx.injectVote(2, fx.cfg.LeadershipTermID, appendPosition, true)
	fx.publisher.Reset()
	require.Equal(t, 1, fx.doWork())
	require.Equal(t, election.ElectionStateInit, fx.election.State())
	require.Equal(t, cause, fx.election.LastError())
	require.Equal(t, election.NullSessionID, fx.election.LogSessionID())
	require.Equal(t, []int64{appendPosition}, fx.host.addLogPublicationCalls)
	require.Empty(t, fx.publisher.Sent)
	require.Equal(t, []election.ElectionState{
		election.ElectionStateLeaderLogReplication,
		election.ElectionStateInit,
	}, observer.states[len(observer.states)-2:])

	// Phase 2: the next win retries the add.
	fx.host.addLogPublicationErr = nil
	fx.canvassTo(election.ElectionStateCandidateBallot)
	fx.injectVote(1, fx.cfg.LeadershipTermID, appendPosition, true)
	fx.injectVote(2, fx.cfg.LeadershipTermID, appendPosition, true)
	fx.doWork()
	require.Equal(t, election.ElectionStateLeaderLogReplication, fx.election.State())
	require.Equal(t, fixtureLogSessionID, fx.election.LogSessionID())
	require.Equal(t, []int64{appendPosition, appendPosition}, fx.host.addLogPublicationCalls)
}

func TestPublishNewLeadershipTerm_commitPositionField(t *testing.T) {
	t.Parallel()

	const quorumPosition int64 = 123
	const recordingID int64 = 842384023
	const appVersion int32 = -98

	fx := leaderLogReplicationFixture(t, quorumPosition, withAppVersion(appVersion))
	fx.host.localLogRecordingID = recordingID

	wantTimestamp := fx.epochClock.nowMs
	fx.doWork()

	require.Len(t, fx.publisher.Sent, 2)
	term := fx.election.LeadershipTermID()
	for _, m := range fx.publisher.Sent {
		require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), m.TemplateID)
		fields := m.Fields.(codecs.NewLeadershipTermFields)
		require.Equal(t, quorumPosition, fields.CommitPosition)
		require.Equal(t, term, fields.LeadershipTermID)
		require.Equal(t, term, fields.NextLeadershipTermID)
		require.Equal(t, election.NullPosition, fields.NextLogPosition)
		require.Equal(t, fx.election.AppendPosition(), fields.NextTermBaseLogPosition)
		require.Equal(t, fx.election.AppendPosition(), fields.TermBaseLogPosition)
		require.Equal(t, fx.election.AppendPosition(), fields.LogPosition)
		require.Equal(t, recordingID, fields.LeaderRecordingID)
		require.Equal(t, wantTimestamp, fields.Timestamp)
		require.Equal(t, fx.election.ThisMemberID(), fields.LeaderMemberID)
		require.Equal(t, fixtureLogSessionID, fields.LogSessionID)
		require.Equal(t, appVersion, fields.AppVersion)
		require.Equal(t, clusterwire.BooleanType_FALSE, fields.IsStartup)
	}

	require.Equal(t, []publishCommitPositionCall{
		{commitPosition: quorumPosition, leadershipTermID: term},
	}, fx.host.publishCommitPositionCalls)
}

func TestOnAppendPosition_leaderRepublishesToLaggingFollower(t *testing.T) {
	t.Parallel()

	fx := leaderLogReplicationFixture(t, 0)
	laggingTerm := fx.election.LeadershipTermID() - 1

	fx.injectAppendPosition(1, laggingTerm, 50)

	require.Equal(t, laggingTerm, fx.member(1).LeadershipTermID)
	require.Equal(t, int64(50), fx.member(1).LogPosition)

	require.Len(t, fx.publisher.Sent, 1)
	sent := fx.publisher.Sent[0]
	require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), sent.TemplateID)
	require.Equal(t, int32(1), sent.Dst.MemberID)
	fields := sent.Fields.(codecs.NewLeadershipTermFields)
	require.Equal(t, laggingTerm, fields.LogLeadershipTermID)
	require.Equal(t, fx.election.LeadershipTermID(), fields.LeadershipTermID)
}

func TestOnCanvassPosition_leaderRepublishesToOlderLogTerm(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		logTerm int64
	}{
		{"log term below the leader log term", testLeadershipTermID - 1},
		{"log term equal to the leader log term", testLeadershipTermID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := leaderLogReplicationFixture(t, 0)
			require.Greater(t, fx.election.LeadershipTermID(), tc.logTerm)

			fx.injectCanvassPosition(1, tc.logTerm, 50)

			require.Equal(t, tc.logTerm, fx.member(1).LeadershipTermID)
			require.Equal(t, int64(50), fx.member(1).LogPosition)

			require.Len(t, fx.publisher.Sent, 1)
			sent := fx.publisher.Sent[0]
			require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), sent.TemplateID)
			require.Equal(t, int32(1), sent.Dst.MemberID)
			fields := sent.Fields.(codecs.NewLeadershipTermFields)
			require.Equal(t, tc.logTerm, fields.LogLeadershipTermID)
			require.Equal(t, fx.election.LeadershipTermID(), fields.LeadershipTermID)
			require.Equal(t, election.ElectionStateLeaderLogReplication, fx.election.State())
		})
	}
}

func TestPublishCommitPositionOnInterval_quorumRegression(t *testing.T) {
	t.Parallel()

	fx := leaderLogReplicationFixture(t, 0)
	fx.host.quorumPosition = 123

	fx.doWork()
	term := fx.election.LeadershipTermID()
	require.Equal(t, []publishCommitPositionCall{
		{commitPosition: 123, leadershipTermID: term},
	}, fx.host.publishCommitPositionCalls)
	require.Len(t, fx.publisher.Sent, 2)

	// The quorum position regressed: the commit position is withheld even
	// after the heartbeat interval, while NewLeadershipTerm republishes.
	fx.host.quorumPosition = 50
	fx.advance(fx.cfg.LeaderHeartbeatIntervalNs)
	fx.doWork()
	require.Equal(t, []publishCommitPositionCall{
		{commitPosition: 123, leadershipTermID: term},
	}, fx.host.publishCommitPositionCalls)
	require.Len(t, fx.publisher.Sent, 4)
	for _, m := range fx.publisher.Sent {
		require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), m.TemplateID)
	}
}

func TestHandleLeaderReplay_firstTickUpdatesThisMember(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = true
	cfg.AppendPosition = cfg.LogPosition
	handler := &recordingAgentHandler{}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderReplay)

	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderInit, e.State())

	member := election.MemberForTest(e, cfg.MemberID)
	require.Equal(t, cfg.LeadershipTermID, member.LeadershipTermID)
	require.Equal(t, cfg.AppendPosition, member.LogPosition)
	require.Equal(t, clock.nowNs, member.TimeOfLastAppendPositionNs)

	// isLeaderStartup reverts to the node's own startup flag on replay start;
	// the next cycle joins the log as leader with it.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Len(t, handler.joinLeaderCalls, 1)
	require.True(t, handler.joinLeaderCalls[0].isStartup)
}

func TestHandleLeaderReplay_doneAtBound(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	spy := &spyLogReplay{position: testAppendPosition, done: true}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplay: func(_, _ int64) election.LogReplay {
			return spy
		},
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderReplay)

	// Install tick.
	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReplay, e.State())
	require.Equal(t, 0, spy.closeCalls)

	// Done at the append bound enters LeaderInit. logPosition equals the append
	// bound. The exit closes the helper. DoWork may be 0 with a done spy. State
	// and closeCalls are the contract.
	_ = e.DoWork(clock.nowNs + 1)
	require.Equal(t, election.ElectionStateLeaderInit, e.State())
	require.Equal(t, e.AppendPosition(), e.LogPosition())
	require.Equal(t, int64(testAppendPosition), e.LogPosition())
	require.Equal(t, 1, spy.closeCalls)

	// Second Close is a no-op after the pointer is cleared.
	e.Close()
	require.Equal(t, 1, spy.closeCalls)
}

func TestHandleLeaderReplay_doneShort(t *testing.T) {
	t.Parallel()

	replayed := int64(testAppendPosition - 50)
	cfg, deps, clock, _, _ := testElectionDeps(t)
	spy := &spyLogReplay{position: replayed, done: true}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplay: func(_, _ int64) election.LogReplay {
			return spy
		},
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderReplay)

	// Seed a live log session so a fall-through publish path would fan out.
	election.SetLogSessionIDForTest(e, fixtureLogSessionID)

	// Install tick.
	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReplay, e.State())
	appendBefore := e.AppendPosition()
	require.Equal(t, 0, spy.closeCalls)

	// Clear install-tick host side effects before the done tick.
	host.publishCommitPositionCalls = nil

	// Done short of the append bound enters Canvass. logPosition is the replayed
	// position. Append position is unchanged. The exit closes the helper and must
	// not publish leadership as a Follower. Incomplete replay must not call
	// HandleError.
	_ = e.DoWork(clock.nowNs + 1)
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, consensus.RoleFollower, host.Role())
	require.Equal(t, int64(replayed), e.LogPosition())
	require.Equal(t, appendBefore, e.AppendPosition())
	require.Equal(t, int64(testAppendPosition), e.AppendPosition())
	require.NotEqual(t, e.AppendPosition(), e.LogPosition())
	require.NoError(t, e.LastError())
	require.Equal(t, 1, spy.closeCalls)
	require.Empty(t, host.publishCommitPositionCalls)

	// Second Close is a no-op after the pointer is cleared.
	e.Close()
	require.Equal(t, 1, spy.closeCalls)
}

func TestHandleLeaderInit_joinLeaderErrorFailsClosed(t *testing.T) {
	t.Parallel()

	joinErr := errors.New("join leader recording failed")
	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
	deps.Host = host
	spy := &recordingLogCoherencySpy{}
	deps.RecordingLog = spy
	handler := &recordingAgentHandler{joinLeaderErr: joinErr}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderInit)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Len(t, handler.joinLeaderCalls, 1)
	require.Empty(t, spy.calls)
}

func TestHandleLeaderInit_joinLeaderSuccessWritesTerm(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, epochClock, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
	deps.Host = host
	spy := &recordingLogCoherencySpy{}
	deps.RecordingLog = spy
	handler := &recordingAgentHandler{}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderInit)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReady, e.State())
	require.Len(t, handler.joinLeaderCalls, 1)
	require.Equal(t, []recordinglog.EnsureCoherentParams{{
		RecordingID:                42,
		InitialLogLeadershipTermID: cfg.LeadershipTermID,
		InitialTermBaseLogPosition: cfg.TermBaseLogPosition,
		LeadershipTermID:           cfg.LeadershipTermID,
		TermBaseLogPosition:        cfg.LogPosition,
		LogPosition:                election.NullValue,
		Timestamp:                  epochClock.nowMs,
	}}, spy.calls)
}

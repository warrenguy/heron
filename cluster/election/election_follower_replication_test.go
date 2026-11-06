package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestHandleFollowerLogReplication_branches(t *testing.T) {
	t.Parallel()

	t.Run("caught up commits replicated term and recanvasses", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		spy := &recordingLogCoherencySpy{}
		deps.RecordingLog = spy
		e := election.NewElection(cfg, deps)

		// Successor term base equals the append position with a known stop
		// position at it: nothing to replicate.
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
			Timestamp:                  clock.nowNs,
		}}, spy.calls)
	})

	t.Run("caught up coherency error resets to init", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		deps.RecordingLog = &recordingLogCoherencySpy{err: errors.New("write failed")}
		e := election.NewElection(cfg, deps)
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))
	})

	t.Run("bare injected state times out without committing", func(t *testing.T) {
		t.Parallel()

		// Without an install the replication stop position is the NullPosition
		// sentinel: the caught-up branch must not commit it as a term, and the
		// unarmed deadline resets the election on the first tick.
		cfg, deps, clock, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateFollowerLogReplication)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))
	})

	t.Run("waits on install deadline then times out", func(t *testing.T) {
		t.Parallel()

		// The installing NewLeadershipTerm sets the replication deadline one
		// heartbeat timeout out.
		cfg, deps, clock, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

		t0 := clock.nowNs
		require.Equal(t, 0, e.DoWork(t0))
		require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

		require.Equal(t, 0, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs-1))
		require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

		require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
		require.Equal(t, election.ElectionStateInit, e.State())
		require.EqualError(t, e.LastError(), "election: timeout awaiting commit position")
	})

	t.Run("first tick builds the helper and sets the deadline from that tick", func(t *testing.T) {
		t.Parallel()

		lr := &countingLogReplicationForTest{done: true}
		fx := newReplicationFixture(t, lr)
		lr.position = fx.stopPosition
		buildNs := fx.clock.nowNs + 7

		require.Equal(t, 1, fx.e.DoWork(buildNs))
		require.Len(t, fx.host.newLogReplicationCalls, 1)
		build := fx.host.newLogReplicationCalls[0]
		require.Equal(t, buildNs, build.nowNs)
		require.Equal(t, fx.stopPosition, build.stopPosition)
		require.Zero(t, lr.doWorkCalls)

		// The install deadline has passed. The deadline from the build holds.
		deadline := buildNs + fx.cfg.LeaderHeartbeatTimeoutNs
		require.Equal(t, 1, fx.e.DoWork(deadline-1))
		require.Equal(t, election.ElectionStateFollowerLogReplication, fx.e.State())

		require.Equal(t, 1, fx.e.DoWork(deadline))
		require.Equal(t, election.ElectionStateInit, fx.e.State())
		require.EqualError(t, fx.e.LastError(), "election: timeout awaiting commit position")
	})

	t.Run("DoWork error enters init and leaves the helper for init to stop", func(t *testing.T) {
		t.Parallel()

		errReplication := errors.New("replication failed")
		lr := &countingLogReplicationForTest{err: errReplication}
		fx := newReplicationFixture(t, lr)
		t0 := fx.clock.nowNs

		require.Equal(t, 1, fx.e.DoWork(t0))
		require.Equal(t, 1, fx.e.DoWork(t0+1))
		require.Equal(t, election.ElectionStateInit, fx.e.State())
		require.ErrorIs(t, fx.e.LastError(), errReplication)
		require.Zero(t, lr.closeCalls)
		require.Empty(t, fx.appendPositions(t))

		// Phase: the next Init pass stops the helper, then prepares.
		require.Equal(t, 1, fx.e.DoWork(t0+2))
		require.Equal(t, 1, lr.closeCalls)
		require.Equal(t, 2, lr.doWorkCalls)
		require.Equal(t, 2, fx.handler.prepareCalls)
		require.Equal(t, election.ElectionStateCanvass, fx.e.State())
	})

	t.Run("rising position sends one append position", func(t *testing.T) {
		t.Parallel()

		lr := &countingLogReplicationForTest{work: 2}
		fx := newReplicationFixture(t, lr)
		lr.position = fx.cfg.AppendPosition + 30
		t0 := fx.clock.nowNs

		require.Equal(t, 1, fx.e.DoWork(t0))
		require.Equal(t, 3, fx.e.DoWork(t0+1))
		require.Equal(t, []int64{lr.position}, fx.appendPositions(t))
		require.Equal(t, lr.position, fx.e.AppendPosition())
		require.Equal(t, t0+1, election.TimeOfLastUpdateNsForTest(fx.e))
	})

	t.Run("same position sends again only on the heartbeat interval", func(t *testing.T) {
		t.Parallel()

		lr := &countingLogReplicationForTest{}
		fx := newReplicationFixture(t, lr)
		lr.position = fx.cfg.AppendPosition + 30
		interval := fx.cfg.LeaderHeartbeatIntervalNs
		t0 := fx.clock.nowNs

		require.Equal(t, 1, fx.e.DoWork(t0))
		require.Equal(t, 1, fx.e.DoWork(t0+1))
		require.Equal(t, 0, fx.e.DoWork(t0+interval))
		require.Equal(t, []int64{lr.position}, fx.appendPositions(t))

		require.Equal(t, 1, fx.e.DoWork(t0+1+interval))
		require.Equal(t, []int64{lr.position, lr.position}, fx.appendPositions(t))
	})

	t.Run("position below the append position does not send", func(t *testing.T) {
		t.Parallel()

		lr := &countingLogReplicationForTest{}
		fx := newReplicationFixture(t, lr)
		lr.position = fx.cfg.AppendPosition - 10
		interval := fx.cfg.LeaderHeartbeatIntervalNs
		t0 := fx.clock.nowNs

		require.Equal(t, 1, fx.e.DoWork(t0))
		require.Equal(t, 0, fx.e.DoWork(t0+1))
		require.Equal(t, 0, fx.e.DoWork(t0+1+2*interval))
		require.Empty(t, fx.appendPositions(t))
		require.Equal(t, fx.cfg.AppendPosition, fx.e.AppendPosition())
	})

	t.Run("refused send leaves the append position and update time", func(t *testing.T) {
		t.Parallel()

		pub := &appendPositionFailPublisher{SbePublisher: &election.SbePublisher{}}
		lr := &countingLogReplicationForTest{}
		fx := newReplicationFixtureWith(t, lr, pub)
		lr.position = fx.cfg.AppendPosition + 30
		updateBefore := election.TimeOfLastUpdateNsForTest(fx.e)
		t0 := fx.clock.nowNs

		require.Equal(t, 1, fx.e.DoWork(t0))
		require.Equal(t, 0, fx.e.DoWork(t0+1))
		require.Equal(t, 1, pub.attempts)
		require.Equal(t, fx.cfg.AppendPosition, fx.e.AppendPosition())
		require.Equal(t, updateBefore, election.TimeOfLastUpdateNsForTest(fx.e))
	})

	t.Run("nil leader does not send", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		publisher := &election.SbePublisher{}
		deps.Publisher = publisher
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateFollowerLogReplication)
		lr := &countingLogReplicationForTest{position: cfg.AppendPosition + 30}
		election.SetLogReplicationForTest(e, lr)

		require.Equal(t, 0, e.DoWork(clock.nowNs))
		require.Equal(t, 1, lr.doWorkCalls)
		require.Empty(t, publisher.Sent)
		require.Equal(t, cfg.AppendPosition, e.AppendPosition())
	})

	doneCases := []struct {
		name        string
		recordingID int64
		wantIDs     []int64
	}{
		{
			name:        "done with notified commit at the append position",
			recordingID: 77,
			wantIDs:     []int64{77},
		},
		{
			name:        "done with an unknown recording id does not store it",
			recordingID: election.NullValue,
		},
	}
	for _, tc := range doneCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lr := &countingLogReplicationForTest{work: 1, done: true, recordingID: tc.recordingID}
			fx := newReplicationFixture(t, lr)
			lr.position = fx.stopPosition
			fx.e.OnCommitPosition(codecs.CommitPositionFields{
				LeadershipTermID: fx.cfg.LeadershipTermID,
				LogPosition:      fx.stopPosition,
				LeaderMemberID:   1,
			})
			t0 := fx.clock.nowNs

			require.Equal(t, 1, fx.e.DoWork(t0))

			// The work count sums helper work, the send at the stop, and the commit.
			require.Equal(t, 3, fx.e.DoWork(t0+1))
			require.Equal(t, election.ElectionStateCanvass, fx.e.State())
			require.Equal(t, fx.stopPosition, fx.e.AppendPosition())
			require.Equal(t, []int64{fx.stopPosition}, fx.appendPositions(t))
			require.Equal(t, tc.wantIDs, fx.host.setLogRecordingIDCalls)
			require.Equal(t, 1, lr.closeCalls)
			require.Equal(t, fx.cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(fx.e))
			require.Len(t, fx.spy.calls, 1)
			entry := fx.spy.calls[0]
			require.Equal(t, fx.cfg.LeadershipTermID, entry.LeadershipTermID)
			require.Equal(t, fx.stopPosition, entry.LogPosition)

			// The handler cleared the helper. Canvass does not run it.
			fx.e.DoWork(t0 + 2)
			require.Equal(t, 1, lr.doWorkCalls)
		})
	}

	t.Run("done below commit sends then times out", func(t *testing.T) {
		t.Parallel()

		lr := &countingLogReplicationForTest{done: true}
		fx := newReplicationFixture(t, lr)
		lr.position = fx.stopPosition
		interval := fx.cfg.LeaderHeartbeatIntervalNs
		t0 := fx.clock.nowNs

		require.Equal(t, 1, fx.e.DoWork(t0))

		// Phase: the first tick after done sends at the helper position.
		require.Equal(t, 1, fx.e.DoWork(t0+1))
		require.Equal(t, []int64{fx.stopPosition}, fx.appendPositions(t))

		// Phase: later ticks run the helper and resend on the interval only.
		require.Equal(t, 0, fx.e.DoWork(t0+2))
		require.Equal(t, 2, lr.doWorkCalls)
		require.Len(t, fx.appendPositions(t), 1)
		require.Equal(t, 1, fx.e.DoWork(t0+1+interval))
		require.Equal(t, []int64{fx.stopPosition, fx.stopPosition}, fx.appendPositions(t))
		require.Empty(t, fx.host.setLogRecordingIDCalls)
		require.Zero(t, lr.closeCalls)

		// Phase: the deadline enters Init and leaves the helper.
		require.Equal(t, 1, fx.e.DoWork(t0+fx.cfg.LeaderHeartbeatTimeoutNs))
		require.Equal(t, election.ElectionStateInit, fx.e.State())
		require.EqualError(t, fx.e.LastError(), "election: timeout awaiting commit position")
		require.Zero(t, lr.closeCalls)
		require.Empty(t, fx.host.setLogRecordingIDCalls)

		// Phase: Init stops the helper.
		require.Equal(t, 1, fx.e.DoWork(t0+fx.cfg.LeaderHeartbeatTimeoutNs+1))
		require.Equal(t, 1, lr.closeCalls)
		require.Equal(t, election.ElectionStateCanvass, fx.e.State())
	})

	// Matching leader traffic refreshes the replication deadline only while a
	// helper is installed. With a nil factory the install-time deadline fires
	// unchanged, so lag still returns to Init under leader heartbeats. The
	// deadline applies to a done helper only.
	commitRefresh := func(e *election.Election, cfg election.ElectionConfig) {
		e.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: cfg.LeadershipTermID,
				LogPosition:      0,
				LeaderMemberID:   1,
			},
		)
	}
	lateTermRefresh := func(e *election.Election, cfg election.ElectionConfig) {
		e.OnNewLeadershipTerm(
			followerLogReplicationFields(cfg, cfg.AppendPosition+50, election.NullPosition),
		)
	}
	deadlineCases := []struct {
		name       string
		withHelper bool
		refresh    func(e *election.Election, cfg election.ElectionConfig)
	}{
		{
			name:    "leader commit position does not extend deadline without helper",
			refresh: commitRefresh,
		},
		{
			name:       "leader commit position extends deadline with helper",
			withHelper: true,
			refresh:    commitRefresh,
		},
		{
			name:    "late leadership term does not extend deadline without helper",
			refresh: lateTermRefresh,
		},
		{
			name:       "late leadership term extends deadline with helper",
			withHelper: true,
			refresh:    lateTermRefresh,
		},
	}
	for _, tc := range deadlineCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			if tc.withHelper {
				// The helper is done above the append position. The notified
				// commit stays 0, so done does not commit and the deadline applies.
				deps.Host = hostWithLogReplication(clock, &countingLogReplicationForTest{
					done:     true,
					position: cfg.AppendPosition + 10,
				})
			}
			e := election.NewElection(cfg, deps)
			driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

			// Install tick: one unit of work builds the helper when the factory is wired.
			t0 := clock.nowNs
			installWork := 0
			if tc.withHelper {
				installWork = 1
			}
			require.Equal(t, installWork, e.DoWork(t0))
			require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

			refreshNs := t0 + cfg.LeaderHeartbeatTimeoutNs - 1
			clock.nowNs = refreshNs
			tc.refresh(e, cfg)

			if !tc.withHelper {
				require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
				require.Equal(t, election.ElectionStateInit, e.State())
				return
			}

			// Refresh moved the deadline one heartbeat past refreshNs. The
			// first tick after the build sends AppendPosition at the helper
			// position.
			require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
			require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
			require.Equal(t, cfg.AppendPosition+10, e.AppendPosition())
			require.Equal(t, 1, e.DoWork(refreshNs+cfg.LeaderHeartbeatTimeoutNs))
			require.Equal(t, election.ElectionStateInit, e.State())
		})
	}
}

func TestHandleFollowerLogReplication_initStopAfterTimeoutReinstalls(t *testing.T) {
	t.Parallel()

	// The timeout leaves the helper for Init. The next Init pass stops it, so a
	// later FOLLOWER_LOG_REPLICATION visit builds a new one.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	var helpers []*countingLogReplicationForTest
	host := &recordingElectionHost{
		clock: clock,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			lr := &countingLogReplicationForTest{
				work:        1,
				done:        true,
				recordingID: int64(91 + len(helpers)),
			}
			helpers = append(helpers, lr)
			return lr
		},
	}
	deps.Host = host
	deps.Handler = &recordingAgentHandler{appendPositionReturn: cfg.AppendPosition}
	e := election.NewElection(cfg, deps)

	// The first Init pass does not stop a helper.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

	t0 := clock.nowNs
	require.Equal(t, 1, e.DoWork(t0))
	require.Len(t, helpers, 1)

	// The helper position 0 sits below the append position, so the tick does not send.
	require.Equal(t, 1, e.DoWork(t0+1))
	require.Len(t, helpers, 1)

	require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Zero(t, helpers[0].closeCalls)

	require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs+1))
	require.Equal(t, 1, helpers[0].closeCalls)
	require.Equal(t, election.ElectionStateCanvass, e.State())

	// Re-enter FOLLOWER_LOG_REPLICATION after leave: factory must run again.
	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Len(t, helpers, 2)
	require.Len(t, host.newLogReplicationCalls, 2)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
}

func TestHandleFollowerLogReplication_caughtUpSkipsBuildThenLaggingBuilds(t *testing.T) {
	t.Parallel()

	// Already at stop: the visit commits without a helper. A later lagging
	// visit builds one.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	installs := 0
	host := &recordingElectionHost{
		clock:               clock,
		localLogRecordingID: 42,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			installs++
			return &countingLogReplicationForTest{}
		},
	}
	deps.Host = host
	deps.RecordingLog = &recordingLogCoherencySpy{}
	e := election.NewElection(cfg, deps)

	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition)
	require.Equal(t, 0, e.DoWork(clock.nowNs))
	require.Zero(t, installs)
	require.Empty(t, host.newLogReplicationCalls)
	require.Equal(t, election.ElectionStateCanvass, e.State())

	// Lagging re-enter after caught-up leave: factory runs.
	//
	// Caught-up commit advances log term; NewLeadershipTerm must match for
	// install.
	logTerm := election.LogLeadershipTermIDForTest(e)
	appendPos := e.AppendPosition()
	fields := installNewLeadershipTermFields(cfg, 1, logTerm)
	fields.LogLeadershipTermID = logTerm
	fields.LeadershipTermID = logTerm
	fields.TermBaseLogPosition = appendPos + 100
	fields.NextLeadershipTermID = logTerm + 1
	fields.NextTermBaseLogPosition = appendPos + 50
	fields.NextLogPosition = election.NullPosition
	e.OnNewLeadershipTerm(fields)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, 1, installs)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
}

// TestHandleFollowerLogReplication_nextTermFailureTruncatesThenReplicates
// asserts recovery from a failed replication of the next term. The next install
// truncates the partial copy to the term base. A second helper copies the term
// again.
func TestHandleFollowerLogReplication_nextTermFailureTruncatesThenReplicates(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	termBase := cfg.AppendPosition
	termEnd := termBase + 100
	failPosition := termBase + 40
	errReplication := errors.New("replication failed")
	var helpers []*countingLogReplicationForTest
	host := &recordingElectionHost{
		clock: clock,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			lr := &countingLogReplicationForTest{}
			if len(helpers) == 0 {
				lr.position = failPosition
				lr.err = errReplication
			}
			helpers = append(helpers, lr)
			return lr
		},
	}
	handler := &recordingAgentHandler{appendPositionReturn: cfg.AppendPosition}
	deps.Host = host
	deps.Handler = handler
	deps.RecordingLog = &recordingLogCoherencySpy{}
	e := election.NewElection(cfg, deps)
	require.Equal(t, 1, e.DoWork(clock.nowNs))

	// Phase: the member at the term base installs the next term and builds a
	// helper. The helper fails above the term base.
	driveToFollowerLogReplication(t, e, cfg, termBase, termEnd)
	t0 := clock.nowNs
	require.Equal(t, 1, e.DoWork(t0))
	require.Equal(t, 1, e.DoWork(t0+1))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.ErrorIs(t, e.LastError(), errReplication)

	// Phase: prepare reads the catalog stop at the failure.
	handler.appendPositionReturn = failPosition
	require.Equal(t, 1, e.DoWork(t0+2))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, failPosition, e.AppendPosition())
	require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))

	// Phase: the leader's next term keyed to the old log term truncates.
	e.OnNewLeadershipTerm(followerLogReplicationFields(cfg, termBase, termEnd))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, []truncateCall{{
		leadershipTermID: cfg.LeadershipTermID,
		logPosition:      termBase,
	}}, host.truncateCalls)
	require.Equal(t, termBase, e.AppendPosition())

	// Phase: prepare reads the truncate stop. The next install builds a second
	// helper for the same term.
	handler.appendPositionReturn = termBase
	require.Equal(t, 1, e.DoWork(t0+3))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	driveToFollowerLogReplication(t, e, cfg, termBase, termEnd)
	require.Equal(t, 1, e.DoWork(t0+4))
	require.Len(t, host.truncateCalls, 1)
	require.Len(t, host.newLogReplicationCalls, 2)
	for _, call := range host.newLogReplicationCalls {
		require.Equal(t, termEnd, call.stopPosition)
	}
}

func TestOnRecordingSignal_initNoOp(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	lr := &countingLogReplicationForTest{}
	election.SetLogReplicationForTest(e, lr)

	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.Zero(t, lr.signalCalls)
}

func TestOnRecordingSignal_withLogReplication(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		recordingID int64
		wantIDs     []int64
	}{
		{name: "known recording id reaches the host", recordingID: 42, wantIDs: []int64{42}},
		{name: "unknown recording id does not reach the host", recordingID: election.NullValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			host := &recordingElectionHost{clock: clock}
			deps.Host = host
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateFollowerLogReplication)

			lr := &countingLogReplicationForTest{recordingID: tc.recordingID}
			election.SetLogReplicationForTest(e, lr)

			e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
			require.Equal(t, 1, lr.signalCalls)
			require.Equal(t, tc.wantIDs, host.setLogRecordingIDCalls)
		})
	}
}

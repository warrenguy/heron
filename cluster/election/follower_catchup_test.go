package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

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

// Not near-live: sticky INIT, near-live re-checked each tick, work 0.
//
// Stall ticks never call catchup start on the host.
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

// CatchupAwait stub: multi-tick work 0, sticky state, no poll/initiated calls.
func TestFollowerCatchupAwait_stubReturnsZero(t *testing.T) {
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

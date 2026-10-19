package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const testRecordingID int64 = 42

func openElectionRecordingLog(t *testing.T) (*recordinglog.Log, string) {
	t.Helper()
	dir := t.TempDir()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	return log, dir
}

func electionWithRealLog(
	t *testing.T,
	recordingID int64,
	log *recordinglog.Log,
	fileSyncLevel recordinglog.FileSyncLevel,
) (*election.Election, election.ElectionConfig, *FakeClock) {
	t.Helper()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	deps.Host = &recordingElectionHost{clock: clock, localLogRecordingID: recordingID}
	deps.RecordingLog = log
	deps.RecordingLogLookup = log
	deps.RecordingLogFileSyncLevel = fileSyncLevel
	return election.NewElection(cfg, deps), cfg, clock
}

// mapArchiveLister is a map-backed ArchiveLister for PlanRecovery in election tests.
type mapArchiveLister map[int64]recordinglog.LogRecordingExtents

func (m mapArchiveLister) ListRecording(id int64) (recordinglog.LogRecordingExtents, error) {
	ext, ok := m[id]
	if !ok {
		return recordinglog.LogRecordingExtents{}, recordinglog.ErrUnknownRecordingID
	}
	return ext, nil
}

func termExtents(start, stop int64) recordinglog.LogRecordingExtents {
	return recordinglog.LogRecordingExtents{
		StartPosition:    start,
		StopPosition:     stop,
		InitialTermID:    0,
		TermBufferLength: 64 * 1024,
		MtuLength:        1408,
		SessionID:        1,
	}
}

func reopenRecordingLog(t *testing.T, dir string) *recordinglog.Log {
	t.Helper()
	reopened, err := recordinglog.Open(dir, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func requireTermIdentity(
	t *testing.T,
	log *recordinglog.Log,
	termID, recordingID, logPosition int64,
) *recordinglog.Entry {
	t.Helper()
	entry := log.FindTermEntry(termID)
	require.NotNil(t, entry)
	require.Equal(t, recordingID, entry.RecordingID)
	require.Equal(t, logPosition, entry.LogPosition)
	return entry
}

func requireDurableTermIdentity(
	t *testing.T,
	log *recordinglog.Log,
	dir string,
	termID, recordingID, logPosition int64,
) *recordinglog.Entry {
	t.Helper()
	require.NoError(t, log.Close())
	return requireTermIdentity(t, reopenRecordingLog(t, dir), termID, recordingID, logPosition)
}

func TestRecordingLog_leaderInitEnsureCoherent(t *testing.T) {
	t.Parallel()

	// FileSyncData exercises Force on the election commit path. Reopen only
	// proves term rows survived the write+close cycle (page cache); it does
	// not distinguish sync levels.
	for _, tc := range []struct {
		name string
		sync recordinglog.FileSyncLevel
	}{
		{name: "FileSyncNone", sync: recordinglog.FileSyncNone},
		{name: "FileSyncData", sync: recordinglog.FileSyncData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			log, dir := openElectionRecordingLog(t)
			e, cfg, clock := electionWithRealLog(t, testRecordingID, log, tc.sync)

			election.SetStateForTest(e, election.ElectionStateLeaderInit)
			require.Equal(t, 1, e.DoWork(clock.nowNs))
			require.Equal(t, election.ElectionStateLeaderReady, e.State())
			require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))

			requireDurableTermIdentity(
				t,
				log,
				dir,
				cfg.LeadershipTermID,
				testRecordingID,
				recordinglog.NullPosition,
			)
		})
	}
}

func TestRecordingLog_followerLogJoinEnsureCoherent(t *testing.T) {
	t.Parallel()

	log, dir := openElectionRecordingLog(t)
	cfg, deps, clock, _, _ := testElectionDeps(t)
	deps.RecordingLog = log
	deps.RecordingLogLookup = log
	adoptedTerm := driveFollowerLogJoinToReady(t, cfg, deps, clock)

	requireDurableTermIdentity(t, log, dir, adoptedTerm, testRecordingID, recordinglog.NullPosition)
}

func TestRecordingLog_followerReplicationCommit(t *testing.T) {
	t.Parallel()

	log, dir := openElectionRecordingLog(t)
	e, cfg, clock := electionWithRealLog(t, testRecordingID, log, recordinglog.FileSyncNone)

	// Successor term already at append stop; commit closed term and recanvass.
	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition)

	require.Equal(t, 0, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	wantTerm := cfg.LeadershipTermID + 1
	require.Equal(t, wantTerm, election.LogLeadershipTermIDForTest(e))

	entry := requireDurableTermIdentity(t, log, dir, wantTerm, testRecordingID, cfg.AppendPosition)
	require.Equal(t, cfg.AppendPosition, entry.TermBaseLogPosition)
}

func TestRecordingLog_nullRecordingIDSkipsWrite(t *testing.T) {
	t.Parallel()

	log, _ := openElectionRecordingLog(t)
	e, _, clock := electionWithRealLog(t, election.NullValue, log, recordinglog.FileSyncNone)

	election.SetStateForTest(e, election.ElectionStateLeaderInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReady, e.State())
	require.Empty(t, log.Entries())
}

func TestRecordingLog_nilLogNoOp(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock, localLogRecordingID: testRecordingID}
	deps.Host = host
	// RecordingLog left nil deliberately.
	e := election.NewElection(cfg, deps)

	election.SetStateForTest(e, election.ElectionStateLeaderInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReady, e.State())
	require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))
}

func TestRecordingLog_planRecoverySeedsElection(t *testing.T) {
	t.Parallel()

	log, _ := openElectionRecordingLog(t)
	const (
		termID      int64 = 3
		termBase    int64 = 0
		termStop    int64 = 500
		recordingID int64 = 55
	)
	require.NoError(t, log.AppendTerm(recordingID, termID, termBase, 1_000))
	require.NoError(t, log.CommitLogPosition(termID, termStop))

	plan, err := log.PlanRecovery(
		mapArchiveLister{recordingID: termExtents(termBase, termStop)},
		recordinglog.NewRecoveryPlanConfig(0),
	)
	require.NoError(t, err)

	cfg, deps, clock, _, _ := testElectionDeps(t)
	// Start from non-matching positions so seed must overwrite.
	cfg.LeadershipTermID = 99
	cfg.TermBaseLogPosition = 99
	cfg.LogPosition = 99
	cfg.AppendPosition = 99
	election.ApplyRecoveryPlan(&cfg, plan)
	require.Equal(t, termID, cfg.LeadershipTermID)
	require.Equal(t, int64(0), cfg.LogPosition)
	require.Equal(t, termStop, cfg.AppendPosition)

	host := &recordingElectionHost{clock: clock, localLogRecordingID: plan.TermRecordingID()}
	deps.Host = host
	deps.RecordingLog = log
	deps.RecordingLogLookup = log
	e := election.NewElection(cfg, deps)
	require.Equal(t, termID, election.LogLeadershipTermIDForTest(e))

	// One more open-term adoption after seed.
	election.SetStateForTest(e, election.ElectionStateLeaderInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReady, e.State())
	requireTermIdentity(t, log, termID, recordingID, termStop)
}

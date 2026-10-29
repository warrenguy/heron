package recordinglog_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/recordinglog"
)

type fakeArchive map[int64]recordinglog.LogRecordingExtents

func (f fakeArchive) ListRecording(id int64) (recordinglog.LogRecordingExtents, error) {
	ext, ok := f[id]
	if !ok {
		return recordinglog.LogRecordingExtents{}, recordinglog.ErrUnknownRecordingID
	}
	return ext, nil
}

func defaultTestExtent() recordinglog.LogRecordingExtents {
	return recordinglog.LogRecordingExtents{
		StartPosition:    0,
		StopPosition:     50_000,
		InitialTermID:    0,
		TermBufferLength: 128 << 20,
		MtuLength:        256 << 10,
		SessionID:        1408,
	}
}

func fakeArchiveWithIDs(ids ...int64) fakeArchive {
	f := make(fakeArchive, len(ids))
	ext := defaultTestExtent()
	for _, id := range ids {
		f[id] = ext
	}
	return f
}

func fakeArchiveForEntries(entries []recordinglog.Entry) fakeArchive {
	f := make(fakeArchive)
	ext := defaultTestExtent()
	for _, e := range entries {
		if e.RecordingID >= 0 {
			f[e.RecordingID] = ext
		}
	}
	return f
}

func snapshotEntry(
	recordingID, leadershipTermID, termBaseLogPosition, logPosition int64,
	serviceID int32,
) recordinglog.Entry {
	return recordinglog.Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    leadershipTermID,
		TermBaseLogPosition: termBaseLogPosition,
		LogPosition:         logPosition,
		Timestamp:           0,
		ServiceID:           serviceID,
		Type:                recordinglog.EntryTypeSnapshot,
		Valid:               true,
	}
}

func recoveryPlanConfig(serviceCount int, replicatedID ...int64) recordinglog.RecoveryPlanConfig {
	cfg := recordinglog.NewRecoveryPlanConfig(serviceCount)
	if len(replicatedID) > 0 {
		cfg.ReplicatedRecordingID = replicatedID[0]
	}
	return cfg
}

type snapshotWant struct {
	recordingID int64
	serviceID   int32
}

func requireSnapshots(t *testing.T, got []recordinglog.Snapshot, want ...snapshotWant) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, w := range want {
		require.Equal(t, w.serviceID, got[i].ServiceID)
		require.Equal(t, w.recordingID, got[i].RecordingID)
	}
}

func standbySnapshotEntry(
	recordingID, leadershipTermID, termBaseLogPosition, logPosition int64,
	serviceID int32,
) recordinglog.Entry {
	entry := snapshotEntry(recordingID, leadershipTermID, termBaseLogPosition, logPosition, serviceID)
	entry.Type = recordinglog.EntryTypeStandbySnapshot
	entry.Timestamp = 1_000_000_000
	entry.ArchiveEndpoint = "remotehost.aeron.io:20002"
	return entry
}

func TestRecoveryPlanStructuredErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     error
		wantMsg string
		wantIs  error
		wantNot error
	}{
		{
			name: "missing service snapshot",
			err: &recordinglog.MissingServiceSnapshotError{
				ServiceIndex:     2,
				LeadershipTermID: 10,
				LogPosition:      777,
			},
			wantMsg: "recordinglog: snapshot missing for service at index 2 " +
				"(leadershipTermId=10, logPosition=777)",
			wantIs:  recordinglog.ErrSnapshotMissingForService,
			wantNot: recordinglog.ErrUnknownRecordingID,
		},
		{
			name: "service id out of range",
			err: &recordinglog.SnapshotServiceIDOutOfRangeError{
				ServiceID:        2,
				ServiceCount:     2,
				LeadershipTermID: 10,
				LogPosition:      777,
			},
			wantMsg: "recordinglog: snapshot service id 2 out of range for serviceCount 2 " +
				"(leadershipTermId=10, logPosition=777)",
			wantIs: recordinglog.ErrSnapshotServiceIDOutOfRange,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.EqualError(t, tc.err, tc.wantMsg)
			require.ErrorIs(t, tc.err, tc.wantIs)
			if tc.wantNot != nil {
				require.NotErrorIs(t, tc.err, tc.wantNot)
			}
		})
	}
}

func TestPlanRecovery_ignoreIncompleteSnapshot(t *testing.T) {
	t.Parallel()

	log, _ := openWithEntries(
		t,
		snapshotEntry(1, 1, 0, 777, 0),
		snapshotEntry(2, 1, 0, 777, recordinglog.ConsensusModuleServiceID),
		snapshotEntry(3, 1, 0, 888, 0),
	)

	plan, err := log.PlanRecovery(fakeArchive{}, recoveryPlanConfig(1))
	require.NoError(t, err)
	requireSnapshots(
		t, plan.Snapshots,
		snapshotWant{2, recordinglog.ConsensusModuleServiceID},
		snapshotWant{1, 0},
	)
}

func TestPlanRecovery_ignoreInvalidMidSnapshot(t *testing.T) {
	t.Parallel()

	log, _ := openWithEntries(
		t,
		snapshotEntry(1, 1, 0, 777, 0),
		snapshotEntry(2, 1, 0, 777, recordinglog.ConsensusModuleServiceID),
		snapshotEntry(3, 1, 0, 888, 0),
		snapshotEntry(4, 1, 0, 888, recordinglog.ConsensusModuleServiceID),
		snapshotEntry(5, 1, 0, 999, 0),
		snapshotEntry(6, 1, 0, 999, recordinglog.ConsensusModuleServiceID),
	)
	require.NoError(t, recordinglog.InvalidateEntry(log, 2))
	require.NoError(t, recordinglog.InvalidateEntry(log, 3))

	plan, err := log.PlanRecovery(fakeArchive{}, recoveryPlanConfig(1))
	require.NoError(t, err)
	requireSnapshots(
		t, plan.Snapshots,
		snapshotWant{6, recordinglog.ConsensusModuleServiceID},
		snapshotWant{5, 0},
	)
}

func TestPlanRecovery_ignoreInvalidTerm(t *testing.T) {
	t.Parallel()

	const removedLeadershipTerm int64 = 11

	log, _ := openWithEntries(
		t,
		termEntryWith(0, 9, 444),
		termEntryWith(0, 10, 666),
		snapshotEntry(1, 10, 666, 777, 0),
		snapshotEntry(2, 10, 666, 777, recordinglog.ConsensusModuleServiceID),
		snapshotEntry(3, 10, 666, 888, 0),
		snapshotEntry(4, 10, 666, 888, recordinglog.ConsensusModuleServiceID),
		termEntryWith(0, removedLeadershipTerm, 999),
	)
	require.NoError(t, recordinglog.InvalidateEntry(log, 6))

	plan, err := log.PlanRecovery(fakeArchiveWithIDs(0), recoveryPlanConfig(1))
	require.NoError(t, err)
	require.NotNil(t, plan.Log)
	require.Equal(t, int64(0), plan.Log.Term.RecordingID)
	require.Equal(t, int64(10), plan.Log.Term.LeadershipTermID)
	require.Equal(t, int64(666), plan.Log.Term.TermBaseLogPosition)
	requireSnapshots(
		t, plan.Snapshots,
		snapshotWant{4, recordinglog.ConsensusModuleServiceID},
		snapshotWant{3, 0},
	)
}

func TestPlanRecovery_excludeStandbySnapshot(t *testing.T) {
	t.Parallel()

	log, _ := openWithEntries(
		t,
		snapshotEntry(1, 1, 0, 1000, 0),
		snapshotEntry(2, 1, 0, 1000, recordinglog.ConsensusModuleServiceID),
		standbySnapshotEntry(3, 2, 1000, 2000, 0),
		standbySnapshotEntry(4, 2, 1000, 2000, recordinglog.ConsensusModuleServiceID),
	)

	plan, err := log.PlanRecovery(fakeArchive{}, recoveryPlanConfig(1))
	require.NoError(t, err)
	requireSnapshots(
		t, plan.Snapshots,
		snapshotWant{2, recordinglog.ConsensusModuleServiceID},
		snapshotWant{1, 0},
	)
}

func TestPlanRecovery_unknownRecordingID(t *testing.T) {
	t.Parallel()

	log, _ := openWithEntries(t, termEntry(42))

	_, err := log.PlanRecovery(fakeArchive{}, recoveryPlanConfig(0))
	require.ErrorIs(t, err, recordinglog.ErrUnknownRecordingID)
}

func TestPlanRecovery_negativeServiceCount(t *testing.T) {
	t.Parallel()

	const replicatedID int64 = 99

	tests := []struct {
		name  string
		setup func(t *testing.T) (*recordinglog.Log, fakeArchive)
		cfg   recordinglog.RecoveryPlanConfig
	}{
		{
			name: "non_empty_log",
			setup: func(t *testing.T) (*recordinglog.Log, fakeArchive) {
				log, _ := openWithEntries(t, termEntry(1))
				return log, fakeArchiveWithIDs(1)
			},
			cfg: recoveryPlanConfig(-1),
		},
		{
			name: "empty_log",
			setup: func(t *testing.T) (*recordinglog.Log, fakeArchive) {
				log, _ := openInDir(t, nil, true)
				return log, fakeArchive{}
			},
			cfg: recoveryPlanConfig(-1),
		},
		{
			name: "empty_log_replicated",
			setup: func(t *testing.T) (*recordinglog.Log, fakeArchive) {
				log, _ := openInDir(t, nil, true)
				return log, fakeArchiveWithIDs(replicatedID)
			},
			cfg: recoveryPlanConfig(-1, replicatedID),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			log, archive := tc.setup(t)
			_, err := log.PlanRecovery(archive, tc.cfg)
			require.ErrorIs(t, err, recordinglog.ErrInvalidServiceCount)
		})
	}
}

func TestPlanRecovery_snapshotsOnly(t *testing.T) {
	t.Parallel()

	log, _ := openWithEntries(
		t,
		snapshotEntry(1, 10, 666, 777, 0),
		snapshotEntry(2, 10, 666, 777, recordinglog.ConsensusModuleServiceID),
	)

	plan, err := log.PlanRecovery(fakeArchive{}, recoveryPlanConfig(1))
	require.NoError(t, err)
	require.Nil(t, plan.Log)
	require.Len(t, plan.Snapshots, 2)
	require.Equal(t, int64(10), plan.LastLeadershipTermID)
	require.Equal(t, int64(666), plan.LastTermBaseLogPosition)
	require.Equal(t, int64(777), plan.AppendedLogPosition)
	require.Equal(t, int64(777), plan.CommittedLogPosition)
}

func TestPlanRecovery_emptyLog_noReplicatedRecording(t *testing.T) {
	t.Parallel()

	log, _ := openInDir(t, nil, true)

	plan, err := log.PlanRecovery(fakeArchive{}, recoveryPlanConfig(0))
	require.NoError(t, err)
	require.Nil(t, plan.Snapshots)
	require.Nil(t, plan.Log)
}

func TestPlanRecovery_serviceCountZero(t *testing.T) {
	t.Parallel()

	log, _ := openWithEntries(
		t,
		snapshotEntry(2, 1, 0, 777, recordinglog.ConsensusModuleServiceID),
		snapshotEntry(1, 1, 0, 777, 0),
	)

	plan, err := log.PlanRecovery(fakeArchive{}, recoveryPlanConfig(0))
	require.NoError(t, err)
	require.Len(t, plan.Snapshots, 1)
	require.Equal(t, recordinglog.ConsensusModuleServiceID, plan.Snapshots[0].ServiceID)
	require.Equal(t, int64(2), plan.Snapshots[0].RecordingID)
}

func TestPlanRecovery_emptyLog_replicatedRecording(t *testing.T) {
	t.Parallel()

	log, _ := openInDir(t, nil, true)

	const replicatedID int64 = 99
	plan, err := log.PlanRecovery(
		fakeArchiveWithIDs(replicatedID),
		recoveryPlanConfig(0, replicatedID),
	)
	require.NoError(t, err)
	require.Nil(t, plan.Snapshots)
	require.NotNil(t, plan.Log)
	require.Equal(t, replicatedID, plan.Log.Term.RecordingID)
	require.Equal(t, int64(recordinglog.NullRecordingID), plan.Log.Term.LeadershipTermID)
	require.Equal(t, defaultTestExtent(), plan.Log.Extent)
}

func TestPlanRecovery_emptyLog_unknownReplicatedRecording(t *testing.T) {
	t.Parallel()

	log, _ := openInDir(t, nil, true)

	_, err := log.PlanRecovery(fakeArchive{}, recoveryPlanConfig(0, 99))
	require.ErrorIs(t, err, recordinglog.ErrUnknownRecordingID)
}

func TestPlanRecovery_snapshotsNilWhenEmpty(t *testing.T) {
	t.Parallel()

	log, _ := openWithEntries(t, termEntry(1))

	plan, err := log.PlanRecovery(fakeArchiveWithIDs(1), recoveryPlanConfig(0))
	require.NoError(t, err)
	require.Nil(t, plan.Snapshots)
	require.NotNil(t, plan.Log)
}

func TestPlanRecovery_startPositionOverride(t *testing.T) {
	t.Parallel()

	log, _ := openWithEntries(
		t,
		termEntry(10),
		snapshotEntry(1, 10, 100, 777, 0),
		snapshotEntry(2, 10, 100, 777, recordinglog.ConsensusModuleServiceID),
	)

	plan, err := log.PlanRecovery(fakeArchiveWithIDs(10), recoveryPlanConfig(1))
	require.NoError(t, err)
	require.NotNil(t, plan.Log)
	require.Equal(t, int64(777), plan.Log.Extent.StartPosition)
	require.Equal(t, int64(50_000), plan.Log.Extent.StopPosition)
}

func TestPlanRecovery_committedLogPosition(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		termLogPosition int64
		serviceCount    int
		wantCommitted   int64
		wantAppended    int64
		wantLastTermID  int64
		wantTermBase    int64
	}{
		{
			name:            "term only",
			termLogPosition: 1234,
			wantCommitted:   1234,
			wantAppended:    50_000,
			wantLastTermID:  10,
			wantTermBase:    666,
		},
		{
			name:            "term overrides snapshot",
			termLogPosition: 1234,
			serviceCount:    1,
			wantCommitted:   1234,
			wantAppended:    50_000,
			wantLastTermID:  10,
			wantTermBase:    666,
		},
		{
			name:            "uncommitted retains snapshot",
			termLogPosition: recordinglog.NullPosition,
			serviceCount:    1,
			wantCommitted:   777,
			wantAppended:    50_000,
			wantLastTermID:  10,
			wantTermBase:    666,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			term := termEntryWith(10, 10, 666)
			term.LogPosition = tc.termLogPosition
			entries := []recordinglog.Entry{term}
			if tc.serviceCount > 0 {
				entries = append(
					entries,
					snapshotEntry(1, 10, 666, 777, 0),
					snapshotEntry(2, 10, 666, 777, recordinglog.ConsensusModuleServiceID),
				)
			}

			log, _ := openWithEntries(t, entries...)
			plan, err := log.PlanRecovery(fakeArchiveWithIDs(10), recoveryPlanConfig(tc.serviceCount))
			require.NoError(t, err)
			require.Equal(t, tc.wantCommitted, plan.CommittedLogPosition)
			require.Equal(t, tc.wantAppended, plan.AppendedLogPosition)
			require.Equal(t, tc.wantLastTermID, plan.LastLeadershipTermID)
			require.Equal(t, tc.wantTermBase, plan.LastTermBaseLogPosition)
		})
	}
}

func TestRecoveryPlan_SeedLogPosition(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(0), recordinglog.RecoveryPlan{}.SeedLogPosition())

	term := termEntryWith(10, 10, 666)
	term.LogPosition = 1234
	log, _ := openWithEntries(t, term)
	plan, err := log.PlanRecovery(fakeArchiveWithIDs(10), recoveryPlanConfig(0))
	require.NoError(t, err)
	require.Equal(t, int64(1234), plan.CommittedLogPosition)
	require.Equal(t, int64(0), plan.SeedLogPosition())

	snapshot := recordinglog.RecoveryPlan{
		Snapshots: []recordinglog.Snapshot{{
			LogPosition: 777,
			ServiceID:   recordinglog.ConsensusModuleServiceID,
		}},
	}
	require.Equal(t, int64(777), snapshot.SeedLogPosition())
}

package cluster_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func recoveryConfig(t *testing.T, dir string) cluster.Config {
	t.Helper()
	return cluster.Config{
		MemberID:                      0,
		ClusterMembers:                goldenClusterMembersSharedLog,
		ClusterDir:                    dir,
		AeronDir:                      "/tmp/aeron",
		ArchiveControlRequestChannel:  testArchiveControlRequestChannel,
		ArchiveControlResponseChannel: testArchiveControlResponseChannel,
	}.Normalize()
}

// fakeLister is a map-backed recordinglog.ArchiveLister for recovery tests.
type fakeLister struct {
	extents map[int64]recordinglog.LogRecordingExtents
	calls   []int64
}

func (f *fakeLister) ListRecording(id int64) (recordinglog.LogRecordingExtents, error) {
	f.calls = append(f.calls, id)
	e, ok := f.extents[id]
	if !ok {
		return recordinglog.LogRecordingExtents{}, recordinglog.ErrUnknownRecordingID
	}
	return e, nil
}

var _ recordinglog.ArchiveLister = (*fakeLister)(nil)

func writeTermLog(t *testing.T, dir string, terms int) {
	t.Helper()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	for i := range terms {
		require.NoError(t, log.AppendTerm(42, int64(i), int64(i)*512, int64(1000*(i+1))))
	}
	require.NoError(t, log.Close())
}

func writeSnapshotLog(t *testing.T, dir string) {
	t.Helper()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	require.NoError(t, log.AppendSnapshot(55, 3, 0, 128, 1000, -1))
	require.NoError(t, log.Close())
}

func extent(start, stop int64) recordinglog.LogRecordingExtents {
	return recordinglog.LogRecordingExtents{
		StartPosition:    start,
		StopPosition:     stop,
		InitialTermID:    0,
		TermBufferLength: 128 << 20,
		MtuLength:        256 << 10,
		SessionID:        1408,
	}
}

func listerWith(id int64, ext recordinglog.LogRecordingExtents) *fakeLister {
	return &fakeLister{
		extents: map[int64]recordinglog.LogRecordingExtents{id: ext},
	}
}

// fdsUnder returns the open descriptors whose target is under dir. The
// directory is this test's own t.TempDir(), so a concurrently running test
// cannot appear in the result.
//
// The prefix carries a trailing separator so a sibling directory whose name
// extends this one cannot match: t.TempDir() numbers subdirectories %03d, so
// /100 and /1000 coexist once a test draws more than 999 times.
func fdsUnder(tb testing.TB, dir string) []string {
	tb.Helper()
	if runtime.GOOS != "linux" {
		tb.Skip("fd targets require linux /proc")
	}
	prefix := dir + string(os.PathSeparator)
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(tb, err)
	var out []string
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			continue
		}
		if strings.HasPrefix(target, prefix) {
			out = append(out, target)
		}
	}
	return out
}

func TestOpenRecovery_coldStartEmptyDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Start from a missing path so MkdirAll creates at 0700.
	clusterDir := filepath.Join(dir, "member")

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, clusterDir),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	info, err := os.Stat(clusterDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())

	for _, name := range []string{"recording.log", election.NodeStateFileName, "cluster.lock"} {
		fi, err := os.Stat(filepath.Join(clusterDir, name))
		require.NoError(t, err, name)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), name)
		if name == "cluster.lock" {
			require.Equal(t, int64(0), fi.Size())
		}
	}
}

func TestOpenRecovery_existingDirKeepsMode(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	clusterDir := filepath.Join(dir, "member")
	require.NoError(t, os.MkdirAll(clusterDir, 0o755))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, clusterDir),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	info, err := os.Stat(clusterDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}

func TestOpenRecovery_mkdirAllFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// ClusterDir is a regular file so MkdirAll fails with ENOTDIR.
	clusterDir := filepath.Join(dir, "notadir")
	require.NoError(t, os.WriteFile(clusterDir, []byte("x"), 0o600))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, clusterDir),
	})
	require.Nil(t, recovery)
	require.Error(t, err)
	require.True(t, errors.Is(err, syscall.ENOTDIR) ||
		strings.Contains(err.Error(), "not a directory") ||
		strings.Contains(err.Error(), "ENOTDIR"),
		"got %v", err)
	require.Contains(t, err.Error(), "cluster: create cluster dir")
}

func TestOpenRecovery_recordingLogOpenFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "recording.log"), 0o700))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.Nil(t, recovery)
	require.ErrorIs(t, err, syscall.EISDIR)
	require.Contains(t, err.Error(), "cluster: open recording log in")
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_nodeStateOpenFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, election.NodeStateFileName),
		[]byte("short"),
		0o600,
	))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.Nil(t, recovery)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
	require.Contains(t, err.Error(), "cluster: open node state file in")
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_lockExcludesSecondOpen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })

	before := fdsUnder(t, dir)
	require.NotEmpty(t, before)

	second, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.Nil(t, second)
	require.ErrorIs(t, err, unix.EWOULDBLOCK)
	require.True(t, strings.HasPrefix(err.Error(), "cluster: lock cluster dir"),
		"got %v", err)
	require.ElementsMatch(t, before, fdsUnder(t, dir))
}

func TestOpenRecovery_lockSymlinkRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "elsewhere")
	require.NoError(t, os.WriteFile(target, nil, 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "cluster.lock")))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.Nil(t, recovery)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "cluster: lock cluster dir"),
		"got %v", err)
	require.True(t, errors.Is(err, unix.ELOOP) ||
		strings.Contains(err.Error(), "too many levels") ||
		strings.Contains(err.Error(), "symbolic link"),
		"got %v", err)
}

func TestOpenRecovery_invalidConfig(t *testing.T) {
	t.Parallel()

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: cluster.Config{},
	})
	require.Nil(t, recovery)
	require.EqualError(t, err, "cluster: ClusterDir is required")
}

func TestRecovery_accessorsBeforeClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	require.NotNil(t, recovery.RecordingLog())
	require.NotNil(t, recovery.NodeState())
	require.Empty(t, recovery.RecordingLog().Entries())
}

func TestRecovery_accessorsAfterClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)

	plan := recovery.Plan()
	electionCfg := recovery.ElectionConfig()
	require.NoError(t, recovery.Close())
	require.Empty(t, fdsUnder(t, dir))

	require.Nil(t, recovery.RecordingLog())
	require.Nil(t, recovery.NodeState())
	// Plan and ElectionConfig remain after Close; handles do not.
	require.Equal(t, plan, recovery.Plan())
	require.Equal(t, electionCfg, recovery.ElectionConfig())
	require.True(t, recovery.IsColdStart())
}

func TestRecovery_doubleClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	require.NoError(t, recovery.Close())
	require.Empty(t, fdsUnder(t, dir))
	require.NoError(t, recovery.Close())
	require.Empty(t, fdsUnder(t, dir))
}

func TestRecovery_nilReceiver(t *testing.T) {
	t.Parallel()

	var recovery *cluster.Recovery
	require.Nil(t, recovery.RecordingLog())
	require.Nil(t, recovery.NodeState())
	require.Equal(t, recordinglog.RecoveryPlan{}, recovery.Plan())
	require.Equal(t, election.ElectionConfig{}, recovery.ElectionConfig())
	require.False(t, recovery.IsColdStart())
	require.NoError(t, recovery.Close())
}

func TestOpenRecovery_reopenAfterClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	firstPlan := first.Plan()
	firstElection := first.ElectionConfig()
	require.NoError(t, first.Close())
	require.Empty(t, fdsUnder(t, dir))

	second, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NotNil(t, second.RecordingLog())
	require.NotNil(t, second.NodeState())
	require.Equal(t, firstPlan, second.Plan())
	require.Equal(t, firstElection, second.ElectionConfig())
}

func TestOpenRecovery_nodeStateSurvivesReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	require.NoError(t, first.NodeState().UpdateCandidateTermID(5, 0, 1))
	require.NoError(t, first.Close())
	require.Empty(t, fdsUnder(t, dir))

	second, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	term, err := second.NodeState().CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, int64(5), term.CandidateTermID())
}

func TestOpenRecovery_nilListerWithEntries(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTermLog(t, dir, 1)

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.Nil(t, recovery)
	require.EqualError(t, err,
		"cluster: recording log in "+dir+" holds 1 entries and needs an ArchiveLister")
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_snapshotLogPrefersListerMessage(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeSnapshotLog(t, dir)

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.Nil(t, recovery)
	require.EqualError(t, err,
		"cluster: recording log in "+dir+" holds 1 entries and needs an ArchiveLister")
	require.NotContains(t, err.Error(), "snapshots")
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_snapshotPlanRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeSnapshotLog(t, dir)

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        recoveryConfig(t, dir),
		ArchiveLister: &fakeLister{extents: map[int64]recordinglog.LogRecordingExtents{}},
	})
	require.Nil(t, recovery)
	require.EqualError(t, err,
		"cluster: recovery plan holds 1 snapshots and snapshot load is not implemented")
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_serviceCountPassThrough(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeSnapshotLog(t, dir)
	cfg := recoveryConfig(t, dir)
	cfg.ServiceCount = 1

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: &fakeLister{extents: map[int64]recordinglog.LogRecordingExtents{}},
	})
	require.Nil(t, recovery)
	require.ErrorIs(t, err, recordinglog.ErrSnapshotMissingForService)
	require.Contains(t, err.Error(), "cluster: plan recovery from")

	var missing *recordinglog.MissingServiceSnapshotError
	require.ErrorAs(t, err, &missing)
	require.Equal(t, 1, missing.ServiceIndex)
	require.Equal(t, int64(3), missing.LeadershipTermID)
	require.Equal(t, int64(128), missing.LogPosition)
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_planEmptyColdStart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	require.True(t, recovery.IsColdStart())
	require.True(t, recovery.Plan().IsEmpty())
	require.Equal(t, int64(-1), recovery.Plan().LastLeadershipTermID)
}

func TestOpenRecovery_oneTermPlan(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTermLog(t, dir, 1)

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        recoveryConfig(t, dir),
		ArchiveLister: listerWith(42, extent(0, 4096)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	plan := recovery.Plan()
	require.False(t, recovery.IsColdStart())
	require.Equal(t, int64(42), plan.TermRecordingID())
	require.Equal(t, int64(0), plan.LastLeadershipTermID)
	require.Equal(t, int64(0), plan.LastTermBaseLogPosition)
	require.Equal(t, int64(4096), plan.AppendedLogPosition)
	require.Equal(t, int64(0), plan.CommittedLogPosition)
	require.NotNil(t, plan.Log)
	require.Empty(t, plan.Snapshots)

	ec := recovery.ElectionConfig()
	require.True(t, ec.IsNodeStartup)
	require.Equal(t, plan.LastLeadershipTermID, ec.LeadershipTermID)
	require.Equal(t, plan.LastTermBaseLogPosition, ec.TermBaseLogPosition)
	require.Equal(t, plan.CommittedLogPosition, ec.LogPosition)
	require.Equal(t, plan.AppendedLogPosition, ec.AppendPosition)
}

func TestOpenRecovery_twoTermsPlan(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTermLog(t, dir, 2)

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        recoveryConfig(t, dir),
		ArchiveLister: listerWith(42, extent(0, 4096)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	plan := recovery.Plan()
	require.False(t, recovery.IsColdStart())
	require.Equal(t, int64(1), plan.LastLeadershipTermID)
	require.Equal(t, int64(512), plan.LastTermBaseLogPosition)
	require.Equal(t, int64(4096), plan.AppendedLogPosition)
	require.Equal(t, int64(0), plan.CommittedLogPosition)
	require.Equal(t, int64(42), plan.TermRecordingID())

	ec := recovery.ElectionConfig()
	require.True(t, ec.IsNodeStartup)
	require.Equal(t, int64(1), ec.LeadershipTermID)
	require.Equal(t, int64(512), ec.TermBaseLogPosition)
	require.Equal(t, int64(0), ec.LogPosition)
	require.Equal(t, int64(4096), ec.AppendPosition)
}

func TestOpenRecovery_activeRecordingSeedLevel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTermLog(t, dir, 1)

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        recoveryConfig(t, dir),
		ArchiveLister: listerWith(42, extent(0, recordinglog.NullPosition)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	// NullPosition append falls back to committed on the election seed.
	require.Equal(t, recordinglog.NullPosition, recovery.Plan().AppendedLogPosition)
	require.Equal(t, recovery.Plan().CommittedLogPosition, recovery.ElectionConfig().AppendPosition)
}

func TestOpenRecovery_coldStartElectionSeed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	ec := recovery.ElectionConfig()
	require.True(t, ec.IsNodeStartup)
	require.Equal(t, election.NullValue, ec.LeadershipTermID)
	require.Equal(t, int64(0), ec.TermBaseLogPosition)
	require.Equal(t, int64(0), ec.LogPosition)
	require.Equal(t, int64(0), ec.AppendPosition)
}

func TestOpenRecovery_coldStartCarriesConfigThrough(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	appointed := int32(1)
	graceful := int32(2)
	cfg := cluster.Config{
		MemberID:                      0,
		ClusterMembers:                goldenClusterMembersSharedLog,
		ClusterDir:                    dir,
		AeronDir:                      "/tmp/aeron",
		ArchiveControlRequestChannel:  testArchiveControlRequestChannel,
		ArchiveControlResponseChannel: testArchiveControlResponseChannel,
		AppointedLeaderID:             &appointed,
		GracefulClosedLeaderID:        &graceful,
		Election: election.ElectionConfig{
			AppVersion:                7,
			ElectionTimeoutNs:         11_000_000_000,
			ElectionStatusIntervalNs:  222_000_000,
			LeaderHeartbeatTimeoutNs:  33_000_000_000,
			LeaderHeartbeatIntervalNs: 444_000_000,
			StartupCanvassTimeoutNs:   55_000_000_000,
			// Caller-set recovery fields must not survive open.
			LeadershipTermID:    99,
			TermBaseLogPosition: 88,
			LogPosition:         77,
			AppendPosition:      66,
			IsNodeStartup:       false,
		},
	}.Normalize()

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{Config: cfg})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	// The whole-struct compare covers every carried-through field.
	want := cfg.Election
	want.IsNodeStartup = true
	want.LeadershipTermID = election.NullValue
	want.TermBaseLogPosition = 0
	want.LogPosition = 0
	want.AppendPosition = 0
	require.Equal(t, want, recovery.ElectionConfig())
}

func TestOpenRecovery_callerPositionsOverwritten(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := recoveryConfig(t, dir)
	cfg.Election.LogPosition = 999
	cfg.Election.AppendPosition = 888
	cfg.Election.TermBaseLogPosition = 777
	cfg.Election.LeadershipTermID = 666

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{Config: cfg})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	ec := recovery.ElectionConfig()
	require.Equal(t, election.NullValue, ec.LeadershipTermID)
	require.Equal(t, int64(0), ec.TermBaseLogPosition)
	require.Equal(t, int64(0), ec.LogPosition)
	require.Equal(t, int64(0), ec.AppendPosition)
}

func TestOpenRecovery_planRecoveryUnknownRecording(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTermLog(t, dir, 1)

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        recoveryConfig(t, dir),
		ArchiveLister: &fakeLister{extents: map[int64]recordinglog.LogRecordingExtents{}},
	})
	require.Nil(t, recovery)
	require.ErrorIs(t, err, recordinglog.ErrUnknownRecordingID)
	require.Contains(t, err.Error(), "cluster: plan recovery from")
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_negativeServiceCountValidateFirst(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	clusterDir := filepath.Join(dir, "member")
	cfg := recoveryConfig(t, clusterDir)
	cfg.ServiceCount = -1

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: listerWith(42, extent(0, 4096)),
	})
	require.Nil(t, recovery)
	require.EqualError(t, err, "cluster: ServiceCount must not be negative")
	_, statErr := os.Stat(clusterDir)
	require.True(t, os.IsNotExist(statErr))
}

func TestOpenRecovery_nonNilListerEmptyLog(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	lister := &fakeLister{extents: map[int64]recordinglog.LogRecordingExtents{}}

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        recoveryConfig(t, dir),
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	require.True(t, recovery.IsColdStart())
	require.Empty(t, lister.calls)
}

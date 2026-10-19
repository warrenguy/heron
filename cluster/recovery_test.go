package cluster_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// termBaseStride is the log-position step between consecutive term bases in
// writeCommittedTermLog fixtures.
const termBaseStride int64 = 4096

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

// writeTermHistory writes terms 0..terms-1 under one recording id.
//
// Write order:
//  1. place each term at base stride*i
//  2. commit the last term to stride*terms when commitLast is true
//
// Returns:
//   - stride*terms as the committed position when commitLast is true
//   - stride*terms as the last term's end when commitLast is false
func writeTermHistory(
	t *testing.T,
	dir string,
	recordingID int64,
	terms int,
	commitLast bool,
) int64 {
	t.Helper()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	for i := range terms {
		require.NoError(t, log.AppendTerm(
			recordingID,
			int64(i),
			termBaseStride*int64(i),
			int64(1000*(i+1)),
		))
	}
	end := termBaseStride * int64(terms)
	if commitLast {
		require.NoError(t, log.CommitLogPosition(int64(terms-1), end))
	}
	require.NoError(t, log.Close())
	return end
}

// writeCommittedTermLog writes terms 0..terms-1 under one recording id. Commits
// the last term.
//
// Returns the committed position (stride*terms).
func writeCommittedTermLog(t *testing.T, dir string, recordingID int64, terms int) int64 {
	t.Helper()
	return writeTermHistory(t, dir, recordingID, terms, true)
}

// restartTB is the testing seam restartCycle needs. Both *testing.T and
// *rapid.T satisfy it. Rapid properties must pass rt so a reopen failure
// shrinks instead of exiting the check goroutine.
type restartTB interface {
	require.TestingT
	Helper()
}

// restartCycle closes recovery, then reopens against the same config. Order is
// close then open. That order is the point of the helper.
//
// Returns the reopened Recovery.
func restartCycle(
	t restartTB,
	recovery *cluster.Recovery,
	cfg cluster.Config,
	lister recordinglog.ArchiveLister,
) *cluster.Recovery {
	t.Helper()
	require.NoError(t, recovery.Close())
	reopened, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	return reopened
}

// openFromHistory writes a committed term log under recording id 42, then opens
// Recovery once. That open is the restart path: durable history already on
// disk, one OpenRecovery.
//
// stop is the Archive extent stop position for recording 42. Registers cleanup
// for the open handle.
//
// Returns the open Recovery and the committed position (stride*terms).
//
// Multi-cycle close/reopen and inter-boot config mutation use restartCycle
// instead.
func openFromHistory(
	t *testing.T,
	terms int,
	stop int64,
) (*cluster.Recovery, int64) {
	t.Helper()

	dir := t.TempDir()
	committed := writeCommittedTermLog(t, dir, 42, terms)
	cfg := recoveryConfig(t, dir)
	lister := listerWith(42, extent(0, stop))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })
	return recovery, committed
}

// errLister returns a fixed error from every ListRecording call.
type errLister struct {
	err error
}

func (e *errLister) ListRecording(int64) (recordinglog.LogRecordingExtents, error) {
	return recordinglog.LogRecordingExtents{}, e.err
}

var _ recordinglog.ArchiveLister = (*errLister)(nil)

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

// openFDCount returns how many open descriptors target path. Callers count a
// second opener of a file this test already holds.
func openFDCount(tb testing.TB, path string) int {
	tb.Helper()
	n := 0
	for _, target := range fdsUnder(tb, filepath.Dir(path)) {
		if target == path {
			n++
		}
	}
	return n
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

// TestRecovery_closeUnlocksWhileDupHoldsOFD asserts Recovery.Close releases the
// exclusive hold with LOCK_UN before the lock file closes.
//
// flock is scoped to the open-file description. A dup of the lock descriptor
// keeps that description alive after Close returns the original handle. Close
// alone would leave the exclusive hold until the last description reference goes
// away. LOCK_UN must free the lock while the dup still holds the description so
// a fresh descriptor takes LOCK_EX|LOCK_NB on the first try.
func TestRecovery_closeUnlocksWhileDupHoldsOFD(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config: recoveryConfig(t, dir),
	})
	require.NoError(t, err)

	fd := cluster.LockFileFd(recovery)
	require.GreaterOrEqual(t, fd, 0)
	dup, err := unix.Dup(fd)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(dup) })

	require.NoError(t, recovery.Close())

	path := filepath.Join(dir, "cluster.lock")
	probe, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(probe) })
	require.NoError(t, unix.Flock(probe, unix.LOCK_EX|unix.LOCK_NB),
		"LOCK_UN on Close must free the lock while a dup still holds the OFD")
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
	require.Equal(t, int64(0), ec.LogPosition)
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

// recoveryCallLog holds one OpenRecovery's stop and list calls in call order.
type recoveryCallLog struct {
	calls []string
}

type loggingLister struct {
	lister *fakeLister
	log    *recoveryCallLog
}

func (l *loggingLister) ListRecording(id int64) (recordinglog.LogRecordingExtents, error) {
	l.log.calls = append(l.log.calls, "list "+strconv.FormatInt(id, 10))
	return l.lister.ListRecording(id)
}

type fakeTermRecordingStopper struct {
	log *recoveryCallLog
	err error
}

func (f *fakeTermRecordingStopper) StopTermRecording(recordingID int64) error {
	f.log.calls = append(f.log.calls, "stop "+strconv.FormatInt(recordingID, 10))
	return f.err
}

func TestOpenRecovery_termRecordingStopper(t *testing.T) {
	t.Parallel()

	newDeps := func(
		t *testing.T,
		dir string,
		stopErr error,
	) (cluster.RecoveryDeps, *recoveryCallLog) {
		t.Helper()
		log := &recoveryCallLog{}
		return cluster.RecoveryDeps{
			Config: recoveryConfig(t, dir),
			ArchiveLister: &loggingLister{
				lister: listerWith(42, extent(0, 4096)),
				log:    log,
			},
			TermRecordingStopper: &fakeTermRecordingStopper{log: log, err: stopErr},
		}, log
	}

	t.Run("term log stops the term recording before the first list", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeTermLog(t, dir, 2)
		deps, log := newDeps(t, dir, nil)

		recovery, err := cluster.OpenRecovery(deps)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, recovery.Close()) })

		require.NotEmpty(t, log.calls)
		require.Equal(t, "stop 42", log.calls[0])
		for _, call := range log.calls[1:] {
			require.Equal(t, "list 42", call)
		}
		require.Equal(t, int64(4096), recovery.Plan().AppendedLogPosition)
	})

	t.Run("empty log sends no stop", func(t *testing.T) {
		t.Parallel()

		deps, log := newDeps(t, t.TempDir(), nil)

		recovery, err := cluster.OpenRecovery(deps)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, recovery.Close()) })

		require.Empty(t, log.calls)
		require.True(t, recovery.IsColdStart())
	})

	t.Run("nil stopper plans as before", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeTermLog(t, dir, 1)
		deps, log := newDeps(t, dir, nil)
		deps.TermRecordingStopper = nil

		recovery, err := cluster.OpenRecovery(deps)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, recovery.Close()) })

		require.NotEmpty(t, log.calls)
		for _, call := range log.calls {
			require.Equal(t, "list 42", call)
		}
		require.Equal(t, int64(4096), recovery.Plan().AppendedLogPosition)
	})

	t.Run("stopper error fails open and releases the lock", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeTermLog(t, dir, 1)
		stopErr := errors.New("archive refused the stop")
		deps, log := newDeps(t, dir, stopErr)

		recovery, err := cluster.OpenRecovery(deps)
		require.Nil(t, recovery)
		require.ErrorIs(t, err, stopErr)
		require.ErrorContains(t, err, "cluster: stop term recording 42 in "+dir)
		require.Equal(t, []string{"stop 42"}, log.calls)
		require.Empty(t, fdsUnder(t, dir))

		// A second open takes the lock: the failed open released it.
		deps, _ = newDeps(t, dir, nil)
		recovery, err = cluster.OpenRecovery(deps)
		require.NoError(t, err)
		require.NoError(t, recovery.Close())
	})
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

func TestOpenRecovery_restartNonEmptyBranch(t *testing.T) {
	t.Parallel()

	recovery, _ := openFromHistory(t, 1, termBaseStride)

	require.False(t, recovery.IsColdStart())
	require.False(t, recovery.Plan().IsEmpty())
	require.Equal(t, int64(42), recovery.Plan().TermRecordingID())
}

func TestOpenRecovery_restartSeedsFromPlan(t *testing.T) {
	t.Parallel()

	// Two terms so LeadershipTermID and TermBaseLogPosition are non-zero. Both
	// differ from cold-start zeros. Extent stops past the committed position.
	// LogPosition is the plan replay start, 0 on every plan.
	recovery, committed := openFromHistory(t, 2, 4*termBaseStride)

	plan := recovery.Plan()
	ec := recovery.ElectionConfig()
	require.True(t, ec.IsNodeStartup)
	require.Equal(t, int64(1), plan.LastLeadershipTermID)
	require.Equal(t, termBaseStride, plan.LastTermBaseLogPosition)
	require.Equal(t, plan.LastLeadershipTermID, ec.LeadershipTermID)
	require.Equal(t, plan.LastTermBaseLogPosition, ec.TermBaseLogPosition)
	require.Equal(t, plan.SeedLogPosition(), ec.LogPosition)
	require.Equal(t, int64(0), ec.LogPosition)
	require.Equal(t, plan.AppendedLogPosition, ec.AppendPosition)
	require.Equal(t, committed, plan.CommittedLogPosition)
}

func TestOpenRecovery_restartActiveExtent(t *testing.T) {
	t.Parallel()

	recovery, committed := openFromHistory(t, 1, recordinglog.NullPosition)

	// NullPosition stop is not a fault. Append seed falls back to committed.
	require.Equal(t, recordinglog.NullPosition, recovery.Plan().AppendedLogPosition)
	require.Equal(t, committed, recovery.Plan().CommittedLogPosition)
	require.Equal(t, recovery.Plan().CommittedLogPosition, recovery.ElectionConfig().AppendPosition)
}

func TestOpenRecovery_restartEmptyExtent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Uncommitted last term keeps CommittedLogPosition at 0.
	// Empty extent (start==stop) must seed AppendPosition to that position.
	// Must not use the NullPosition fallback, which would return 0.
	writeTermLog(t, dir, 1)
	cfg := recoveryConfig(t, dir)
	lister := listerWith(42, extent(termBaseStride, termBaseStride))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	require.Equal(t, int64(0), recovery.Plan().CommittedLogPosition)
	require.Equal(t, termBaseStride, recovery.Plan().AppendedLogPosition)
	require.Equal(t, termBaseStride, recovery.ElectionConfig().AppendPosition)
}

func TestOpenRecovery_restartOverwritesCallerAppendPosition(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	committed := writeCommittedTermLog(t, dir, 42, 1)
	cfg := recoveryConfig(t, dir)
	lister := listerWith(42, extent(0, committed*2))

	first, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	require.NoError(t, first.Close())

	cfg.Election.AppendPosition = 999
	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	require.Equal(t, recovery.Plan().AppendedLogPosition, recovery.ElectionConfig().AppendPosition)
	require.NotEqual(t, int64(999), recovery.ElectionConfig().AppendPosition)
}

func TestOpenRecovery_restartCarriesConfigThrough(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	committed := writeCommittedTermLog(t, dir, 42, 1)
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
	lister := listerWith(42, extent(0, committed*2))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovery.Close()) })

	// Whole-struct compare: non-recovery fields carried; recovery fields
	// overwritten from the plan (same shape as cold-start sibling).
	plan := recovery.Plan()
	want := cfg.Election
	want.IsNodeStartup = true
	want.LeadershipTermID = plan.LastLeadershipTermID
	want.TermBaseLogPosition = plan.LastTermBaseLogPosition
	want.LogPosition = plan.SeedLogPosition()
	want.AppendPosition = plan.AppendedLogPosition
	require.Equal(t, want, recovery.ElectionConfig())
}

func TestOpenRecovery_restartListerErrorWrapped(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCommittedTermLog(t, dir, 42, 1)
	injected := errors.New("lister transport down")

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        recoveryConfig(t, dir),
		ArchiveLister: &errLister{err: injected},
	})
	require.Nil(t, recovery)
	require.ErrorIs(t, err, injected)
	require.True(t, strings.HasPrefix(err.Error(), "cluster: plan recovery from "),
		"got %v", err)
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_restartOrderingRule(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	committed := writeCommittedTermLog(t, dir, 42, 1)
	cfg := recoveryConfig(t, dir)
	lister := listerWith(42, extent(0, committed))

	first, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	// Idempotent Close: releases lock if an assert fails before the hand-written
	// close path runs.
	t.Cleanup(func() { require.NoError(t, first.Close()) })

	// Reopen without close must fail.
	// Written out by hand so the violation is deliberate.
	// restartCycle always closes first.
	second, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.Nil(t, second)
	require.ErrorIs(t, err, unix.EWOULDBLOCK)

	require.NoError(t, first.Close())
	reopened, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.False(t, reopened.IsColdStart())
}

func TestOpenRecovery_restartSnapshotBesideTermsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCommittedTermLog(t, dir, 42, 1)
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	require.NoError(t, log.AppendSnapshot(55, 0, 0, termBaseStride, 2000, -1))
	require.NoError(t, log.Close())

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        recoveryConfig(t, dir),
		ArchiveLister: listerWith(42, extent(0, termBaseStride)),
	})
	require.Nil(t, recovery)
	require.EqualError(t, err,
		"cluster: recovery plan holds 1 snapshots and snapshot load is not implemented")
	require.Empty(t, fdsUnder(t, dir))
}

func TestOpenRecovery_restartRepeatsStably(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	committed := writeCommittedTermLog(t, dir, 42, 1)
	cfg := recoveryConfig(t, dir)
	lister := listerWith(42, extent(0, committed))

	recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
		Config:        cfg,
		ArchiveLister: lister,
	})
	require.NoError(t, err)
	// Cleanup closes the Recovery currently held in the recovery variable.
	// Still closes after restartCycle reassigns the variable mid-loop.
	t.Cleanup(func() {
		if recovery != nil {
			require.NoError(t, recovery.Close())
		}
	})

	firstPlan := recovery.Plan()
	firstElection := recovery.ElectionConfig()
	firstCold := recovery.IsColdStart()
	firstEntries := len(recovery.RecordingLog().Entries())

	for range 3 {
		recovery = restartCycle(t, recovery, cfg, lister)
		require.Equal(t, firstPlan, recovery.Plan())
		require.Equal(t, firstElection, recovery.ElectionConfig())
		require.Equal(t, firstCold, recovery.IsColdStart())
		require.Len(t, recovery.RecordingLog().Entries(), firstEntries)
	}
}

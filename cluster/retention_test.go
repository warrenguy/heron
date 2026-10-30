package cluster_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const (
	retentionStart int64 = 64
	retentionStop  int64 = 65536
)

var errRetentionTruncate = errors.New("truncate failed")

func TestRetainLatestSnapshot(t *testing.T) {
	t.Parallel()

	t.Run("two groups, ServiceCount 2", func(t *testing.T) {
		t.Parallel()
		fx := writeTwoGroups(t, 2)
		planBefore := planRetention(t, fx.dir, fx.archive, 2)

		result, err := retain(t, fx.dir, 2, fx.archive)
		require.NoError(t, err)
		requireTruncatedToStart(t, fx.archive, fx.older)
		require.Equal(t, fx.keptTerm, result.LeadershipTermID)
		require.Equal(t, fx.keptPosition, result.LogPosition)
		require.Equal(t, len(fx.older), result.InvalidatedEntries)
		require.Equal(t, len(fx.older), result.DroppedEntries)
		require.Equal(t, fx.older, result.TruncatedRecordingIDs)
		require.Empty(t, result.SkippedRecordingIDs)
		requireNeverTruncated(t, fx.archive, fx.termRecordingID)
		requireNeverTruncated(t, fx.archive, fx.kept...)
		requireKeptLog(t, fx.dir, fx.kept, 2)
		require.Equal(t, planBefore, planRetention(t, fx.dir, fx.archive, 2))
	})

	t.Run("ServiceCount 0", func(t *testing.T) {
		t.Parallel()
		fx := writeTwoGroups(t, 0)
		result, err := retain(t, fx.dir, 0, fx.archive)
		require.NoError(t, err)
		requireTruncatedToStart(t, fx.archive, fx.older)
		require.Equal(t, fx.keptTerm, result.LeadershipTermID)
		require.Equal(t, fx.keptPosition, result.LogPosition)
		require.Equal(t, fx.older, result.TruncatedRecordingIDs)
		requireKeptLog(t, fx.dir, fx.kept, 2)
	})

	t.Run("single group", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 2)
		kept := appendFullGroup(t, log, 2, 1, 4096, 5120, 2500, newID(14))
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		archive.putRows(kept, fullServiceIDs(2), retentionStart, retentionStop)
		before := readRecordingLog(t, dir)

		result, err := retain(t, dir, 2, archive)
		require.NoError(t, err)
		require.Empty(t, archive.truncates)
		require.Equal(t, before, readRecordingLog(t, dir))
		requireNoBackup(t, dir)
		require.Equal(t, int64(1), result.LeadershipTermID)
		require.Equal(t, int64(5120), result.LogPosition)
		require.Zero(t, result.InvalidatedEntries)
		require.Zero(t, result.DroppedEntries)
		require.Empty(t, result.TruncatedRecordingIDs)
		require.Empty(t, result.SkippedRecordingIDs)
	})

	t.Run("no snapshot", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 2)
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		before := readRecordingLog(t, dir)

		_, err := retain(t, dir, 2, archive)
		require.ErrorIs(t, err, cluster.ErrNoSnapshotToRetain)
		require.Equal(t, before, readRecordingLog(t, dir))
		require.Empty(t, archive.truncates)
	})

	t.Run("group without a log term", func(t *testing.T) {
		t.Parallel()
		dir, archive := newRetentionDir(t)
		log := createLog(t, dir)
		older := appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		kept := appendFullGroup(t, log, 2, 0, 0, 200, 1600, newID(21))
		require.NoError(t, log.Close())
		archive.putRows(older, fullServiceIDs(2), retentionStart, retentionStop)
		archive.putRows(kept, fullServiceIDs(2), retentionStart, retentionStop)
		before := readRecordingLog(t, dir)

		_, err := retain(t, dir, 2, archive)
		require.ErrorIs(t, err, cluster.ErrIncompleteSnapshotGroup)
		require.Contains(t, err.Error(), "no log term")
		require.Equal(t, before, readRecordingLog(t, dir))
		require.Empty(t, archive.truncates)
		requireSnapshotsValid(t, dir, older)
	})

	t.Run("incomplete group", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 1)
		older := appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		appendSnapshotRows(t, log, 0, 0, 200, 1600, newID(20), 0, -1)
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		before := readRecordingLog(t, dir)

		_, err := retain(t, dir, 2, archive)
		require.ErrorIs(t, err, cluster.ErrIncompleteSnapshotGroup)
		require.Contains(t, err.Error(), "service 1")
		require.Equal(t, before, readRecordingLog(t, dir))
		require.Empty(t, archive.truncates)
		requireSnapshotsValid(t, dir, older)
	})

	t.Run("invalid walked row", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 1)
		appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		found, err := log.InvalidateLatestSnapshot()
		require.NoError(t, err)
		require.True(t, found)
		next := newID(30)
		appendSnapshotRows(t, log, 0, 0, 100, 1800, next, 0, -1)
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		before := readRecordingLog(t, dir)

		_, err = retain(t, dir, 2, archive)
		require.ErrorIs(t, err, cluster.ErrIncompleteSnapshotGroup)
		require.Contains(t, err.Error(), "service 1")
		require.Equal(t, before, readRecordingLog(t, dir))
		require.Empty(t, archive.truncates)
	})

	t.Run("failed-write rows", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 1)
		below := appendSnapshotRows(t, log, 0, 0, 100, 1100, newID(11), 1, 0)
		kept := appendFullGroup(t, log, 2, 0, 0, 500, 1500, newID(21))
		above := appendSnapshotRows(t, log, 0, 0, 900, 1900, newID(31), 1, 0)
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		archive.putRows(below, []int32{1, 0}, retentionStart, retentionStop)
		archive.putRows(kept, fullServiceIDs(2), retentionStart, retentionStop)
		archive.putRows(above, []int32{1, 0}, retentionStart, retentionStop)

		result, err := retain(t, dir, 2, archive)
		require.NoError(t, err)
		superseded := slices.Concat(below, above)
		slices.Sort(superseded)
		require.Equal(t, superseded, result.TruncatedRecordingIDs)
		requireTruncatedToStart(t, archive, superseded)
		requireKeptLog(t, dir, kept, 1)
		ids := reloadRecordingIDs(t, dir)
		require.NotContains(t, ids, below[0])
		require.NotContains(t, ids, above[0])
	})

	t.Run("duplicate on the highest service at the kept key", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 1)
		older := appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		next := newID(21)
		firstService1 := appendSnapshotRows(t, log, 0, 0, 500, 1600, next, 1)
		retake := appendFullGroup(t, log, 2, 0, 0, 500, 1700, next)
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		archive.putRows(older, fullServiceIDs(2), retentionStart, retentionStop)
		archive.putRows(firstService1, []int32{1}, retentionStart, retentionStop)
		archive.putRows(retake, fullServiceIDs(2), retentionStart, retentionStop)

		result, err := retain(t, dir, 2, archive)
		require.NoError(t, err)
		require.Equal(t, older, result.TruncatedRecordingIDs)
		requireNeverTruncated(t, archive, firstService1[0], retake[0])
		requireSnapshotsValid(t, dir, []int64{firstService1[0], retake[0]})
		requireKeptLogContains(t, dir, retake)
	})

	t.Run("duplicates on two services at the kept key", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 1)
		appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		next := newID(21)
		appendSnapshotRows(t, log, 0, 0, 500, 1600, next, 1, 0)
		appendFullGroup(t, log, 2, 0, 0, 500, 1700, next)
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		before := readRecordingLog(t, dir)

		_, err := retain(t, dir, 2, archive)
		require.ErrorIs(t, err, cluster.ErrIncompleteSnapshotGroup)
		require.Contains(t, err.Error(), "service 1")
		require.Equal(t, before, readRecordingLog(t, dir))
		require.Empty(t, archive.truncates)
	})

	t.Run("invalidated newest group", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 1)
		older := appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		newer := appendFullGroup(t, log, 2, 0, 0, 500, 1800, newID(21))
		found, err := log.InvalidateLatestSnapshot()
		require.NoError(t, err)
		require.True(t, found)
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		archive.putRows(older, fullServiceIDs(2), retentionStart, retentionStop)
		archive.putRows(newer, fullServiceIDs(2), retentionStart, retentionStop)

		result, err := retain(t, dir, 2, archive)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.LeadershipTermID)
		require.Equal(t, int64(100), result.LogPosition)
		require.Equal(t, newer, result.TruncatedRecordingIDs)
		requireTruncatedToStart(t, archive, newer)
		requireKeptLog(t, dir, older, 1)
	})

	t.Run("unknown, empty and other-stream candidates", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 1)
		older := appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		kept := appendFullGroup(t, log, 2, 0, 0, 500, 1800, newID(21))
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		archive.put(older[1], streamForService(0), retentionStart, retentionStart)
		archive.put(older[2], streamForService(-1)+1000, retentionStart, retentionStop)
		archive.putRows(kept, fullServiceIDs(2), retentionStart, retentionStop)

		result, err := retain(t, dir, 2, archive)
		require.NoError(t, err)
		require.Empty(t, archive.truncates)
		require.Equal(t, older, result.SkippedRecordingIDs)
		require.Equal(t, len(older), result.DroppedEntries)
		requireKeptLog(t, dir, kept, 1)
	})

	t.Run("active candidate", func(t *testing.T) {
		t.Parallel()
		dir, log, archive := newTermLog(t, 10, 1)
		older := appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		kept := appendFullGroup(t, log, 2, 0, 0, 500, 1800, newID(21))
		require.NoError(t, log.Close())
		archive.put(10, 1, retentionStart, retentionStop)
		archive.put(older[0], streamForService(1), retentionStart, retentionStop)
		archive.put(older[1], streamForService(0), retentionStart, recordinglog.NullPosition)
		archive.put(older[2], streamForService(-1), retentionStart, retentionStop)
		archive.putRows(kept, fullServiceIDs(2), retentionStart, retentionStop)

		_, err := retain(t, dir, 2, archive)
		require.ErrorIs(t, err, cluster.ErrSnapshotRecordingActive)
		require.Contains(t, err.Error(), "12")
		require.Equal(t, []int64{older[0], older[2]}, archive.truncates)
		require.Equal(t, 3, countInvalidSnapshots(t, dir))
		requireNoBackup(t, dir)

		archive.setStop(older[1], retentionStop)
		result, err := retain(t, dir, 2, archive)
		require.NoError(t, err)
		requireTruncatedToStart(t, archive, older)
		require.Contains(t, result.TruncatedRecordingIDs, older[1])
		requireKeptLog(t, dir, kept, 1)
	})

	t.Run("truncate error", func(t *testing.T) {
		t.Parallel()
		fx := writeTwoGroups(t, 2)
		fx.archive.truncateErr = func(attempt int) error {
			if attempt == 0 {
				return errRetentionTruncate
			}
			return nil
		}

		_, err := retain(t, fx.dir, 2, fx.archive)
		require.ErrorIs(t, err, errRetentionTruncate)
		require.Contains(t, err.Error(), "11")
		require.Contains(t, err.Error(), "truncate snapshot recording")
		require.Empty(t, fx.archive.truncates)
		require.Equal(t, len(fx.older), countInvalidSnapshots(t, fx.dir))
		requireNoBackup(t, fx.dir)

		fx.archive.truncateErr = nil
		retry, err := retain(t, fx.dir, 2, fx.archive)
		require.NoError(t, err)
		require.Zero(t, retry.InvalidatedEntries)
		require.Equal(t, len(fx.older), retry.DroppedEntries)
		requireTruncatedToStart(t, fx.archive, fx.older)
		requireKeptLog(t, fx.dir, fx.kept, 2)

		beforeLog := readRecordingLog(t, fx.dir)
		beforeBak := readOptional(t, backupPath(fx.dir))
		result, err := retain(t, fx.dir, 2, fx.archive)
		require.NoError(t, err)
		require.Equal(t, fx.keptTerm, result.LeadershipTermID)
		require.Equal(t, fx.keptPosition, result.LogPosition)
		require.Zero(t, result.InvalidatedEntries)
		require.Zero(t, result.DroppedEntries)
		require.Empty(t, result.TruncatedRecordingIDs)
		require.Empty(t, result.SkippedRecordingIDs)
		require.Equal(t, beforeLog, readRecordingLog(t, fx.dir))
		require.Equal(t, beforeBak, readOptional(t, backupPath(fx.dir)))
	})

	t.Run("done context", func(t *testing.T) {
		t.Parallel()
		fx := writeTwoGroups(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fx.archive.onList = func(id int64) error {
			if slices.Contains(fx.older, id) {
				cancel()
			}
			return nil
		}

		_, err := cluster.RetainLatestSnapshot(ctx, cluster.SnapshotRetentionConfig{
			ClusterDir:   fx.dir,
			ServiceCount: 2,
			Archive:      fx.archive,
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, fx.archive.truncates)
		require.Equal(t, len(fx.older), countInvalidSnapshots(t, fx.dir))
		requireNoBackup(t, fx.dir)
	})

	t.Run("term recording in a snapshot row", func(t *testing.T) {
		t.Parallel()
		const termRecordingID = int64(10)
		dir, log, archive := newTermLog(t, termRecordingID, 1)
		appendSnapshotRows(t, log, 0, 0, 50, 1400, newID(termRecordingID), 0)
		older := appendFullGroup(t, log, 2, 0, 0, 100, 1500, newID(11))
		kept := appendFullGroup(t, log, 2, 0, 0, 500, 1800, newID(21))
		require.NoError(t, log.Close())
		archive.put(termRecordingID, 1, retentionStart, retentionStop)
		archive.putRows(older, fullServiceIDs(2), retentionStart, retentionStop)
		archive.putRows(kept, fullServiceIDs(2), retentionStart, retentionStop)

		_, err := retain(t, dir, 2, archive)
		require.NoError(t, err)
		requireNeverTruncated(t, archive, termRecordingID)
		require.Equal(t, retentionStop, archive.recording(t, termRecordingID).StopPosition)
		requireTruncatedToStart(t, archive, older)
	})

	t.Run("kept recording unknown", func(t *testing.T) {
		t.Parallel()
		fx := writeTwoGroups(t, 2)
		delete(fx.archive.recordings, fx.kept[0])
		before := readRecordingLog(t, fx.dir)

		_, err := retain(t, fx.dir, 2, fx.archive)
		require.ErrorIs(t, err, cluster.ErrIncompleteSnapshotGroup)
		require.Contains(t, err.Error(), "unknown recording")
		require.Contains(t, err.Error(), "14")
		require.Empty(t, fx.archive.truncates)
		require.Equal(t, before, readRecordingLog(t, fx.dir))
		requireSnapshotsValid(t, fx.dir, fx.older)
	})

	t.Run("kept recording empty", func(t *testing.T) {
		t.Parallel()
		fx := writeTwoGroups(t, 2)
		fx.archive.setStop(fx.kept[2], retentionStart)
		before := readRecordingLog(t, fx.dir)

		_, err := retain(t, fx.dir, 2, fx.archive)
		require.ErrorIs(t, err, cluster.ErrIncompleteSnapshotGroup)
		require.Contains(t, err.Error(), "empty recording")
		require.Contains(t, err.Error(), "16")
		require.Empty(t, fx.archive.truncates)
		require.Equal(t, before, readRecordingLog(t, fx.dir))
		requireSnapshotsValid(t, fx.dir, fx.older)
	})

	t.Run("kept recording active", func(t *testing.T) {
		t.Parallel()
		fx := writeTwoGroups(t, 2)
		fx.archive.setStop(fx.kept[2], recordinglog.NullPosition)
		before := readRecordingLog(t, fx.dir)

		_, err := retain(t, fx.dir, 2, fx.archive)
		require.ErrorIs(t, err, cluster.ErrSnapshotRecordingActive)
		require.Contains(t, err.Error(), "16")
		require.Empty(t, fx.archive.truncates)
		require.Equal(t, before, readRecordingLog(t, fx.dir))

		fx.archive.setStop(fx.kept[2], retentionStop)
		_, err = retain(t, fx.dir, 2, fx.archive)
		require.NoError(t, err)
		requireTruncatedToStart(t, fx.archive, fx.older)
		requireKeptLog(t, fx.dir, fx.kept, 2)
	})

	t.Run("ServiceCount below the group", func(t *testing.T) {
		t.Parallel()
		fx := writeTwoGroups(t, 2)
		before := readRecordingLog(t, fx.dir)

		_, err := retain(t, fx.dir, 1, fx.archive)
		require.ErrorIs(t, err, recordinglog.ErrSnapshotServiceIDOutOfRange)
		var outOfRange *recordinglog.SnapshotServiceIDOutOfRangeError
		require.ErrorAs(t, err, &outOfRange)
		require.Equal(t, int32(1), outOfRange.ServiceID)
		require.Contains(t, err.Error(), "snapshot row out of range")
		require.Empty(t, fx.archive.truncates)
		require.Equal(t, before, readRecordingLog(t, fx.dir))
	})

	t.Run("running member", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		log := createLog(t, dir)
		require.NoError(t, log.AppendTerm(42, 0, 0, 1))
		require.NoError(t, log.AppendSnapshot(
			43, 0, 0, 128, 2, recordinglog.ConsensusModuleServiceID,
		))
		require.NoError(t, log.Close())

		cfg := recoveryConfig(t, dir)
		cfg.ServiceCount = 0
		recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
			Config:        cfg,
			ArchiveLister: listerWith(42, extent(0, 4096)),
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, recovery.Close()) })

		before := readRecordingLog(t, dir)
		archive := newRetentionArchive()
		_, err = retain(t, dir, 0, archive)
		require.ErrorIs(t, err, unix.EWOULDBLOCK)
		require.Equal(t, before, readRecordingLog(t, dir))
		require.Empty(t, archive.lists)
		require.Empty(t, archive.truncates)
	})

	t.Run("missing recording log", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		_, err := retain(t, dir, 0, newRetentionArchive())
		require.ErrorIs(t, err, os.ErrNotExist)
		require.Contains(t, err.Error(), "open recording log")
		_, statErr := os.Stat(filepath.Join(dir, "recording.log"))
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("config", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			noDir   bool
			cfg     cluster.SnapshotRetentionConfig
			wantErr string
		}{
			{
				name:    "empty ClusterDir",
				noDir:   true,
				cfg:     cluster.SnapshotRetentionConfig{Archive: newRetentionArchive()},
				wantErr: "cluster: retain latest snapshot: ClusterDir is required",
			},
			{
				name: "negative ServiceCount",
				cfg: cluster.SnapshotRetentionConfig{
					ServiceCount: -1,
					Archive:      newRetentionArchive(),
				},
				wantErr: "cluster: retain latest snapshot: ServiceCount must not be negative",
			},
			{
				name:    "nil Archive",
				wantErr: "cluster: retain latest snapshot: Archive is required",
			},
			{
				name: "level 3",
				cfg: cluster.SnapshotRetentionConfig{
					RecordingLogFileSyncLevel: 3,
					Archive:                   newRetentionArchive(),
				},
				wantErr: "cluster: RecordingLogFileSyncLevel must be FileSyncData or FileSyncFull",
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				cfg := tc.cfg
				if !tc.noDir {
					cfg.ClusterDir = t.TempDir()
				}
				_, err := cluster.RetainLatestSnapshot(context.Background(), cfg)
				require.EqualError(t, err, tc.wantErr)
				if archive, ok := cfg.Archive.(*retentionArchive); ok {
					require.Empty(t, archive.lists)
				}
				if !tc.noDir {
					requireNoLock(t, cfg.ClusterDir)
				}
			})
		}
	})
}

type twoGroupFixture struct {
	dir             string
	archive         *retentionArchive
	termRecordingID int64
	older           []int64
	kept            []int64
	keptTerm        int64
	keptPosition    int64
}

func writeTwoGroups(t *testing.T, serviceCount int) twoGroupFixture {
	t.Helper()
	const termRecordingID = int64(10)
	dir, log, archive := newTermLog(t, termRecordingID, 2)
	next := newID(11)
	older := appendFullGroup(t, log, serviceCount, 0, 0, 1024, 1500, next)
	kept := appendFullGroup(t, log, serviceCount, 1, 4096, 5120, 2500, next)
	require.NoError(t, log.Close())
	services := fullServiceIDs(serviceCount)
	archive.put(termRecordingID, 1, retentionStart, retentionStop)
	archive.putRows(older, services, retentionStart, retentionStop)
	archive.putRows(kept, services, retentionStart, retentionStop)
	return twoGroupFixture{
		dir:             dir,
		archive:         archive,
		termRecordingID: termRecordingID,
		older:           older,
		kept:            kept,
		keptTerm:        1,
		keptPosition:    5120,
	}
}

func newRetentionDir(t *testing.T) (string, *retentionArchive) {
	t.Helper()
	return t.TempDir(), newRetentionArchive()
}

func newTermLog(
	t *testing.T,
	recordingID int64,
	terms int,
) (string, *recordinglog.Log, *retentionArchive) {
	t.Helper()
	dir, archive := newRetentionDir(t)
	log := createLog(t, dir)
	appendTerms(t, log, recordingID, terms)
	return dir, log, archive
}

func createLog(t *testing.T, dir string) *recordinglog.Log {
	t.Helper()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	return log
}

func appendTerms(t *testing.T, log *recordinglog.Log, recordingID int64, terms int) {
	t.Helper()
	for i := range terms {
		require.NoError(t, log.AppendTerm(
			recordingID,
			int64(i),
			int64(i)*4096,
			int64(1000*(i+1)),
		))
	}
}

func newID(start int64) *int64 {
	id := start
	return &id
}

func fullServiceIDs(serviceCount int) []int32 {
	ids := make([]int32, serviceCount+1)
	for row := range ids {
		ids[row] = int32(serviceCount - 1 - row)
	}
	return ids
}

func appendFullGroup(
	t *testing.T,
	log *recordinglog.Log,
	serviceCount int,
	termID int64,
	termBase int64,
	position int64,
	timestamp int64,
	nextID *int64,
) []int64 {
	t.Helper()
	return appendSnapshotRows(
		t, log, termID, termBase, position, timestamp, nextID, fullServiceIDs(serviceCount)...,
	)
}

func appendSnapshotRows(
	t *testing.T,
	log *recordinglog.Log,
	termID int64,
	termBase int64,
	position int64,
	timestamp int64,
	nextID *int64,
	serviceIDs ...int32,
) []int64 {
	t.Helper()
	ids := make([]int64, 0, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		id := *nextID
		*nextID++
		require.NoError(t, log.AppendSnapshot(
			id, termID, termBase, position, timestamp, serviceID,
		))
		ids = append(ids, id)
	}
	return ids
}

func streamForService(serviceID int32) int32 {
	return 100 + serviceID
}

func retain(
	t retentionTB,
	dir string,
	serviceCount int,
	archive *retentionArchive,
) (cluster.SnapshotRetentionResult, error) {
	t.Helper()
	return cluster.RetainLatestSnapshot(context.Background(), cluster.SnapshotRetentionConfig{
		ClusterDir:   dir,
		ServiceCount: serviceCount,
		Archive:      archive,
	})
}

type retentionTB interface {
	require.TestingT
	Helper()
}

func openRetentionPlan(
	t retentionTB,
	dir string,
	archive *retentionArchive,
	serviceCount int,
) (recordinglog.RecoveryPlan, error) {
	t.Helper()
	log, err := recordinglog.Open(dir, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, log.Close()) }()
	return log.PlanRecovery(
		archive.planLister(),
		recordinglog.NewRecoveryPlanConfig(serviceCount),
	)
}

func planRetention(
	t retentionTB,
	dir string,
	archive *retentionArchive,
	serviceCount int,
) recordinglog.RecoveryPlan {
	t.Helper()
	plan, err := openRetentionPlan(t, dir, archive, serviceCount)
	require.NoError(t, err)
	return plan
}

func readRecordingLog(t retentionTB, dir string) []byte {
	t.Helper()
	buf, err := os.ReadFile(filepath.Join(dir, "recording.log"))
	require.NoError(t, err)
	return buf
}

func backupPath(dir string) string {
	return filepath.Join(dir, "recording.log.bak")
}

func readOptional(t retentionTB, path string) []byte {
	t.Helper()
	buf, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	return buf
}

func requireNoBackup(t *testing.T, dir string) {
	t.Helper()
	require.Nil(t, readOptional(t, backupPath(dir)))
}

func requireNoLock(t *testing.T, dir string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, "cluster.lock"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func reloadEntries(t retentionTB, dir string) []recordinglog.Entry {
	t.Helper()
	entries, err := recordinglog.Reload(filepath.Join(dir, "recording.log"))
	require.NoError(t, err)
	return entries
}

func reloadRecordingIDs(t *testing.T, dir string) []int64 {
	t.Helper()
	var ids []int64
	for _, entry := range reloadEntries(t, dir) {
		ids = append(ids, entry.RecordingID)
	}
	return ids
}

func requireKeptLog(t *testing.T, dir string, kept []int64, termCount int) {
	t.Helper()
	entries := reloadEntries(t, dir)
	require.Len(t, entries, termCount+len(kept))
	for _, entry := range entries {
		require.True(t, entry.Valid)
		if entry.Type == recordinglog.EntryTypeSnapshot {
			require.Contains(t, kept, entry.RecordingID)
			continue
		}
		require.Equal(t, recordinglog.EntryTypeTerm, entry.Type)
	}
}

func requireKeptLogContains(t *testing.T, dir string, kept []int64) {
	t.Helper()
	ids := reloadRecordingIDs(t, dir)
	for _, id := range kept {
		require.Contains(t, ids, id)
	}
}

func requireSnapshotsValid(t *testing.T, dir string, ids []int64) {
	t.Helper()
	entries := reloadEntries(t, dir)
	for _, id := range ids {
		found := false
		for _, entry := range entries {
			if entry.Type != recordinglog.EntryTypeSnapshot || entry.RecordingID != id {
				continue
			}
			require.True(t, entry.Valid)
			found = true
		}
		require.True(t, found, "recording %d", id)
	}
}

func countInvalidSnapshots(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	for _, entry := range reloadEntries(t, dir) {
		if entry.Type == recordinglog.EntryTypeSnapshot && !entry.Valid {
			n++
		}
	}
	return n
}

func requireTruncatedToStart(t *testing.T, archive *retentionArchive, ids []int64) {
	t.Helper()
	for _, id := range ids {
		fields := archive.recording(t, id)
		require.Equal(t, fields.StartPosition, fields.StopPosition, "recording %d", id)
	}
}

func requireNeverTruncated(t *testing.T, archive *retentionArchive, ids ...int64) {
	t.Helper()
	truncated := archive.truncates
	for _, id := range ids {
		require.NotContains(t, truncated, id)
	}
}

type retentionArchive struct {
	recordings       map[int64]codecs.RecordingDescriptorFields
	lists            []int64
	truncates        []int64
	truncateAttempts int
	truncateErr      func(attempt int) error
	onList           func(id int64) error
}

func newRetentionArchive() *retentionArchive {
	return &retentionArchive{
		recordings: map[int64]codecs.RecordingDescriptorFields{},
	}
}

func (a *retentionArchive) put(id int64, stream int32, start, stop int64) {
	a.recordings[id] = codecs.RecordingDescriptorFields{
		RecordingID:      id,
		StartPosition:    start,
		StopPosition:     stop,
		StreamID:         stream,
		InitialTermID:    7,
		TermBufferLength: 65536,
		MtuLength:        1408,
		SessionID:        9,
	}
}

func (a *retentionArchive) putRows(ids []int64, services []int32, start, stop int64) {
	for i, id := range ids {
		a.put(id, streamForService(services[i]), start, stop)
	}
}

func (a *retentionArchive) setStop(id, stop int64) {
	fields := a.recordings[id]
	fields.StopPosition = stop
	a.recordings[id] = fields
}

func (a *retentionArchive) recording(
	t retentionTB,
	id int64,
) codecs.RecordingDescriptorFields {
	t.Helper()
	fields, ok := a.recordings[id]
	require.True(t, ok, "recording %d", id)
	return fields
}

func (a *retentionArchive) ListRecording(
	ctx context.Context,
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	a.lists = append(a.lists, recordingID)
	if err := ctx.Err(); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}
	if a.onList != nil {
		if err := a.onList(recordingID); err != nil {
			return codecs.RecordingDescriptorFields{}, err
		}
	}
	fields, ok := a.recordings[recordingID]
	if !ok {
		return codecs.RecordingDescriptorFields{}, &archive.Exception{
			ErrorCode: archive.ErrorUnknownRecording,
		}
	}
	return fields, nil
}

func (a *retentionArchive) TruncateRecording(
	ctx context.Context,
	recordingID int64,
	position int64,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	attempt := a.truncateAttempts
	a.truncateAttempts++
	if a.truncateErr != nil {
		if err := a.truncateErr(attempt); err != nil {
			return 0, err
		}
	}
	fields, ok := a.recordings[recordingID]
	if !ok {
		return 0, &archive.Exception{ErrorCode: archive.ErrorUnknownRecording}
	}
	fields.StopPosition = position
	a.recordings[recordingID] = fields
	a.truncates = append(a.truncates, recordingID)
	return 1, nil
}

func (a *retentionArchive) planLister() recordinglog.ArchiveLister {
	return archive.NewLister(context.Background(), a)
}

package recordinglog_test

import (
	"cmp"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/recordinglog"
)

type manifestEntry struct {
	RecordingID         int64  `json:"recordingId"`
	LeadershipTermID    int64  `json:"leadershipTermId"`
	TermBaseLogPosition int64  `json:"termBaseLogPosition"`
	LogPosition         int64  `json:"logPosition"`
	Timestamp           int64  `json:"timestamp"`
	ServiceID           int32  `json:"serviceId"`
	Type                int32  `json:"type"`
	Valid               bool   `json:"valid"`
	ArchiveEndpoint     string `json:"archiveEndpoint"`
	Position            int64  `json:"position"`
	Index               int32  `json:"index"`
}

type manifestFixture struct {
	File    string          `json:"file"`
	Entries []manifestEntry `json:"entries"`
}

type manifest struct {
	Fixtures []manifestFixture `json:"fixtures"`
}

func loadManifest(t *testing.T) manifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "fixtures.json"))
	require.NoError(t, err)

	var m manifest
	require.NoError(t, json.Unmarshal(raw, &m))
	require.NotEmpty(t, m.Fixtures)
	return m
}

func manifestEntries(t *testing.T, file string) []manifestEntry {
	t.Helper()

	m := loadManifest(t)
	for _, fixture := range m.Fixtures {
		if fixture.File == file {
			require.NotEmpty(t, fixture.Entries, "fixture %q has no entries", file)
			return fixture.Entries
		}
	}
	t.Fatalf("manifest missing fixture %q", file)
	return nil
}

func manifestToEntry(entry manifestEntry) recordinglog.Entry {
	return recordinglog.Entry{
		RecordingID:         entry.RecordingID,
		LeadershipTermID:    entry.LeadershipTermID,
		TermBaseLogPosition: entry.TermBaseLogPosition,
		LogPosition:         entry.LogPosition,
		Timestamp:           entry.Timestamp,
		ServiceID:           entry.ServiceID,
		Type:                entry.Type,
		ArchiveEndpoint:     entry.ArchiveEndpoint,
		Valid:               entry.Valid,
		Position:            entry.Position,
		Index:               entry.Index,
	}
}

// TestReload_goldenFixtures decodes committed recording.log fixtures against fixtures.json.
func TestReload_goldenFixtures(t *testing.T) {
	t.Parallel()

	m := loadManifest(t)

	for _, fixture := range m.Fixtures {
		t.Run(fixture.File, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, fixture.Entries)

			entries, err := recordinglog.Reload(filepath.Join("testdata", fixture.File))
			require.NoError(t, err)

			want := make([]recordinglog.Entry, len(fixture.Entries))
			for i, e := range fixture.Entries {
				want[i] = manifestToEntry(e)
			}

			require.Equal(t, want, entries)
		})
	}
}

// replayManifestEntries drives a fresh Log writer through the fixture's entries
// in file order (ascending index) and returns the resulting file bytes. Term
// logPosition is derived by the writer, not supplied, matching the append API.
func replayManifestEntries(t *testing.T, entries []manifestEntry) []byte {
	t.Helper()

	log, path := newLog(t)

	ordered := slices.Clone(entries)
	slices.SortFunc(ordered, func(a, b manifestEntry) int {
		return cmp.Compare(a.Index, b.Index)
	})

	for _, e := range ordered {
		switch e.Type {
		case recordinglog.EntryTypeTerm:
			require.NoError(t, log.AppendTerm(
				e.RecordingID, e.LeadershipTermID, e.TermBaseLogPosition, e.Timestamp,
			))
		case recordinglog.EntryTypeStandbySnapshot:
			require.NoError(t, log.AppendStandbySnapshot(
				e.RecordingID, e.LeadershipTermID, e.TermBaseLogPosition,
				e.LogPosition, e.Timestamp, e.ServiceID, e.ArchiveEndpoint,
			))
		case recordinglog.EntryTypeSnapshot:
			require.NoError(t, log.AppendSnapshot(
				e.RecordingID, e.LeadershipTermID, e.TermBaseLogPosition,
				e.LogPosition, e.Timestamp, e.ServiceID,
			))
		default:
			t.Fatalf("unhandled entry type %d", e.Type)
		}
	}
	require.NoError(t, log.Close())

	written, err := os.ReadFile(path)
	require.NoError(t, err)
	return written
}

// TestWriter_byteIdenticalToGolden reconstructs a Java-written golden through the
// Go writer and asserts the bytes match exactly, proving record alignment,
// padding, field order, and endpoint encoding agree with the reference.
//
// Only mixed-entries qualifies: it is append-only and fully zero-padded. The
// others do not qualify:
//   - standby-then-term: the Java writer reuses its buffer, so records after
//     a standby row carry stale endpoint bytes in padding. The Go writer
//     zero-pads.
//   - tiebreaks: carries the same stale padding after its standby row. It
//     also calls invalidateEntry, which the append replay lacks.
//   - invalid-snapshot: needs a term to invalidate against. The single-entry
//     fixture omits it.
//   - null-slot: needs the deferred remove path.
//   - truncated-tail: holds a torn sample, not clean writer output.
func TestWriter_byteIdenticalToGolden(t *testing.T) {
	t.Parallel()

	const reproducible = "mixed-entries.recordinglog"

	entries := manifestEntries(t, reproducible)

	golden, err := os.ReadFile(filepath.Join("testdata", reproducible))
	require.NoError(t, err)
	require.NotEmpty(t, golden)

	written := replayManifestEntries(t, entries)
	require.Equal(t, golden, written)
}

// TestReload_tiebreakFixtureOrder asserts the sorted file-index sequence of
// the tiebreaks fixture. The manifest follows a regen. A changed comparator
// would still pass TestReload_goldenFixtures. This expectation lives in Go.
//
// Each step of the sequence asserts one tiebreak:
//   - 0 then 5: the TERM row sorts before every snapshot in its term. The
//     module row at log position 256 sorts before the rows at 512.
//   - 4, 3, 2, 6, 1: at position 512 the standby row sorts first by type,
//     then service 1, then both service 0 rows in file order, then the
//     module row.
//   - 8, 9: two TERM rows for term 1 sort by file index, not timestamp.
//   - 7: the term 1 module row sorts after both TERM rows.
func TestReload_tiebreakFixtureOrder(t *testing.T) {
	t.Parallel()

	entries, err := recordinglog.Reload(filepath.Join("testdata", "tiebreaks.recordinglog"))
	require.NoError(t, err)

	indexes := make([]int32, len(entries))
	for i, e := range entries {
		indexes[i] = e.Index
	}
	require.Equal(t, []int32{0, 5, 4, 3, 2, 6, 1, 8, 9, 7}, indexes)
}

// TestPlanRecovery_fixtureExpectations asserts PlanRecovery output over
// committed golden logs. Expectations live here, not in fixtures.json: the
// manifest is generator output and regen would drop hand-authored values.
func TestPlanRecovery_fixtureExpectations(t *testing.T) {
	t.Parallel()

	cases := []struct {
		file                string
		serviceCount        int
		snapshotCount       int
		logRecordingID      int64
		logLeadershipTermID int64
	}{
		{
			file:                "mixed-entries.recordinglog",
			serviceCount:        0,
			snapshotCount:       0,
			logRecordingID:      1,
			logLeadershipTermID: 7,
		},
		{
			file:                "standby-then-term.recordinglog",
			serviceCount:        0,
			snapshotCount:       0,
			logRecordingID:      10,
			logLeadershipTermID: 2,
		},
		{
			// PlanRecovery skips the invalidated TERM row for term 1. The term 1
			// module row forms the newest snapshot group.
			file:                "tiebreaks.recordinglog",
			serviceCount:        0,
			snapshotCount:       1,
			logRecordingID:      1,
			logLeadershipTermID: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			t.Parallel()

			golden, err := os.ReadFile(filepath.Join("testdata", tc.file))
			require.NoError(t, err)
			require.NotEmpty(t, golden)

			log, _ := openInDir(t, golden, false)

			plan, err := log.PlanRecovery(
				fakeArchiveForEntries(log.Entries()),
				recoveryPlanConfig(tc.serviceCount),
			)
			require.NoError(t, err)
			require.Len(t, plan.Snapshots, tc.snapshotCount)
			require.NotNil(t, plan.Log)
			require.Equal(t, tc.logRecordingID, plan.Log.Term.RecordingID)
			require.Equal(t, tc.logLeadershipTermID, plan.Log.Term.LeadershipTermID)
		})
	}
}

func TestCompact_javaTiebreakFixture(t *testing.T) {
	t.Parallel()

	const goldenFile = "tiebreaks.recordinglog"
	rows := manifestEntries(t, goldenFile)

	golden, err := os.ReadFile(filepath.Join("testdata", goldenFile))
	require.NoError(t, err)
	log, path := openInDir(t, golden, false)

	dropped, err := log.Compact()
	require.NoError(t, err)
	require.Equal(t, 1, dropped)

	var kept []recordinglog.Entry
	var sourceIndexes []int32
	for _, row := range rows {
		if !row.Valid {
			require.Equal(t, int32(8), row.Index)
			require.Equal(t, recordinglog.EntryTypeTerm, row.Type)
			continue
		}
		sourceIndexes = append(sourceIndexes, row.Index)
		kept = append(kept, manifestToEntry(row))
	}
	want, file := compactedEntries(kept)
	require.Equal(t, []int32{0, 5, 4, 3, 2, 6, 1, 9, 7}, sourceIndexes)
	require.Equal(t, want, log.Entries())
	requireReloadMatchesLog(t, path, log)
	requireFileEqual(t, path, file)

	plan, err := log.PlanRecovery(
		fakeArchiveForEntries(log.Entries()),
		recoveryPlanConfig(0),
	)
	require.NoError(t, err)
	require.Len(t, plan.Snapshots, 1)
	require.NotNil(t, plan.Log)
	require.Equal(t, int64(1), plan.Log.Term.RecordingID)
	require.Equal(t, int64(1), plan.Log.Term.LeadershipTermID)
}

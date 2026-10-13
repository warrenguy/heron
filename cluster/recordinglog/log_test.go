package recordinglog_test

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/internal/testutil"
)

func openInDir(t *testing.T, content []byte, createNew bool) (*recordinglog.Log, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "recording.log")
	if content != nil {
		require.NoError(t, os.WriteFile(path, content, 0o600))
	}
	log, err := recordinglog.Open(dir, createNew)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, log.Close()) })
	return log, path
}

// openWithEntries encodes entries to a fresh recording.log, opens it, registers
// Close, and returns the open Log with the file path for Reload comparisons.
func openWithEntries(t *testing.T, entries ...recordinglog.Entry) (*recordinglog.Log, string) {
	t.Helper()
	content := make([]byte, 0, len(entries)*int(recordinglog.RecordAlignment))
	for _, e := range entries {
		content = append(content, recordinglog.EncodeEntry(e)...)
	}
	return openInDir(t, content, false)
}

func requireReloadMatchesLog(tb testing.TB, path string, log *recordinglog.Log) {
	tb.Helper()
	reloaded, err := recordinglog.Reload(path)
	require.NoError(tb, err)
	entries := log.Entries()
	if entries == nil {
		entries = []recordinglog.Entry{}
	}
	require.Equal(tb, reloaded, entries)
}

func TestOpen_newFileEmptyCacheZeroSeed(t *testing.T) {
	t.Parallel()
	log, _ := openInDir(t, nil, true)
	require.Empty(t, log.Entries())
	require.Equal(t, int32(0), log.NextEntryIndex())
}

func TestSyncDirectory_exportedAndOpenCreateNewStillCreatesFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	recordinglog.SyncDirectory(dir)

	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	require.NoError(t, log.Close())

	_, err = os.Stat(filepath.Join(dir, "recording.log"))
	require.NoError(t, err)
}

func TestOpen_missingFileWithoutCreate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, err := recordinglog.Open(dir, false)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpen_reloadsExistingFile(t *testing.T) {
	t.Parallel()
	snap := recordinglog.Entry{
		RecordingID:         6,
		LeadershipTermID:    11,
		TermBaseLogPosition: 100,
		LogPosition:         300,
		Timestamp:           2000,
		Type:                recordinglog.EntryTypeSnapshot,
		Valid:               true,
	}
	var termSnap []byte
	termSnap = append(termSnap, recordinglog.EncodeEntry(termEntry(5))...)
	termSnap = append(termSnap, recordinglog.EncodeEntry(snap)...)

	for _, tc := range []struct {
		name      string
		content   []byte
		createNew bool
	}{
		{"existing file seeds from reload", termSnap, false},
		{"create new over existing file reloads", twoTerms(5, 5), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log, path := openInDir(t, tc.content, tc.createNew)
			require.Equal(t, int32(2), log.NextEntryIndex())
			requireReloadMatchesLog(t, path, log)
		})
	}
}

func TestOpen_decodeErrorClosesFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "recording.log")
	require.NoError(t, os.WriteFile(path, twoTerms(1, 2), 0o600))

	log, err := recordinglog.Open(dir, false)
	require.ErrorIs(t, err, recordinglog.ErrTermRecordingIDMismatch)
	require.Nil(t, log)
	require.Zero(t, openFDCount(t, path))
}

func TestOpen_nextEntryIndexCountsNullSlots(t *testing.T) {
	t.Parallel()
	// null-slot.recordinglog holds three aligned slots, one of them a NULL_VALUE
	// removed slot, so the seed is the entry ordinal (3), not the valid count (2).
	fixture, err := os.ReadFile(filepath.Join("testdata", "null-slot.recordinglog"))
	require.NoError(t, err)
	log, _ := openInDir(t, fixture, false)
	require.Len(t, log.Entries(), 2)
	require.Equal(t, int32(3), log.NextEntryIndex())
}

func TestOpen_truncatedTailAppendPadsLikeReference(t *testing.T) {
	t.Parallel()
	buf := make([]byte, recordinglog.RecordAlignment+30)
	copy(buf, recordinglog.EncodeEntry(termEntry(9)))

	log, path := openInDir(t, buf, false)
	require.NoError(t, log.AppendTerm(9, 11, recordinglog.RecordAlignment, 2000))
	require.NoError(t, log.Close())

	_, err := recordinglog.Reload(path)
	require.ErrorIs(t, err, recordinglog.ErrTermRecordingIDMismatch)
}

func TestForce_levels(t *testing.T) {
	t.Parallel()
	for _, level := range []recordinglog.FileSyncLevel{
		recordinglog.FileSyncNone,
		recordinglog.FileSyncData,
		recordinglog.FileSyncFull,
	} {
		t.Run(strconv.Itoa(int(level)), func(t *testing.T) {
			t.Parallel()
			log, _ := openInDir(t, recordinglog.EncodeEntry(termEntry(7)), false)
			require.NoError(t, log.Force(level))
		})
	}
}

func TestClose_thenForceFails(t *testing.T) {
	t.Parallel()
	log, _ := openInDir(t, recordinglog.EncodeEntry(termEntry(1)), false)
	require.NoError(t, log.Close())
	require.Error(t, log.Force(recordinglog.FileSyncData))
}

func TestClose_idempotent(t *testing.T) {
	t.Parallel()
	log, _ := openInDir(t, recordinglog.EncodeEntry(termEntry(1)), false)
	require.NoError(t, log.Close())
	require.NoError(t, log.Close())
}

func TestTermRecordingID_noTermEntryReportsNull(t *testing.T) {
	t.Parallel()

	t.Run("empty log", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.Equal(t, recordinglog.NullRecordingID, log.TermRecordingID())
	})

	t.Run("snapshot only", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.NoError(t, log.AppendSnapshot(6, 1, 100, 200, 1000, 0))
		require.Equal(t, recordinglog.NullRecordingID, log.TermRecordingID())
	})
}

func TestTermRecordingID_closedLogKeepsValue(t *testing.T) {
	t.Parallel()

	log, _ := newLog(t)
	require.NoError(t, log.AppendTerm(42, 1, 0, 1000))
	require.Equal(t, int64(42), log.TermRecordingID())
	require.NoError(t, log.Close())
	require.Equal(t, int64(42), log.TermRecordingID())
}

func TestEnsureCoherent_recordingIDRegimes(t *testing.T) {
	t.Parallel()

	const (
		established int64 = 100
		fresh       int64 = 200
		termID      int64 = 1
		termBase    int64 = 0
		timestamp   int64 = 1000
	)

	t.Run("new term under established id appends", func(t *testing.T) {
		t.Parallel()
		log, path := newLog(t)
		require.NoError(t, log.AppendTerm(established, termID, termBase, timestamp))
		require.NoError(t, log.CommitLogPosition(termID, termBase))

		require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
			RecordingID:         established,
			LeadershipTermID:    termID + 1,
			TermBaseLogPosition: termBase,
			LogPosition:         recordinglog.NullPosition,
			Timestamp:           timestamp,
			FileSyncLevel:       recordinglog.FileSyncNone,
		}))
		require.Len(t, log.Entries(), 2)
		require.Equal(t, established, findTerm(t, log.Entries(), termID+1).RecordingID)
		require.Equal(t, established, log.TermRecordingID())
		requireReloadMatchesLog(t, path, log)
	})

	t.Run("new term under fresh id mismatches", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.NoError(t, log.AppendTerm(established, termID, termBase, timestamp))
		require.NoError(t, log.CommitLogPosition(termID, termBase))

		err := log.EnsureCoherent(recordinglog.EnsureCoherentParams{
			RecordingID:         fresh,
			LeadershipTermID:    termID + 1,
			TermBaseLogPosition: termBase,
			LogPosition:         recordinglog.NullPosition,
			Timestamp:           timestamp,
			FileSyncLevel:       recordinglog.FileSyncNone,
		})
		require.ErrorIs(t, err, recordinglog.ErrTermRecordingIDMismatch)
		require.Len(t, log.Entries(), 1)
		require.Equal(t, established, log.TermRecordingID())
	})

	t.Run("existing term under fresh id defers mismatch", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.NoError(t, log.AppendTerm(established, termID, termBase, timestamp))

		// Target already indexed: EnsureCoherent skips append, so a fresh
		// recording id is never compared.
		require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
			RecordingID:         fresh,
			LeadershipTermID:    termID,
			TermBaseLogPosition: termBase,
			LogPosition:         recordinglog.NullPosition,
			Timestamp:           timestamp,
			FileSyncLevel:       recordinglog.FileSyncNone,
		}))
		require.Len(t, log.Entries(), 1)
		require.Equal(t, established, findTerm(t, log.Entries(), termID).RecordingID)
		require.Equal(t, established, log.TermRecordingID())
	})
}

func newLog(t *testing.T) (*recordinglog.Log, string) {
	t.Helper()
	return openInDir(t, nil, true)
}

func TestAppend_roundtripEqualsReload(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)

	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.NoError(t, log.AppendSnapshot(9, 5, 100, 200, 1500, 0))
	require.NoError(
		t,
		log.AppendStandbySnapshot(9, 5, 100, 200, 1600, 0, "aeron:udp?endpoint=standby:8020"),
	)

	requireReloadMatchesLog(t, path, log)
}

func TestAppend_rejectsEntryIndexOverflow(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		append func(*recordinglog.Log) error
	}{
		{
			name: "term",
			append: func(log *recordinglog.Log) error {
				return log.AppendTerm(1, 0, 0, 0)
			},
		},
		{
			name: "snapshot",
			append: func(log *recordinglog.Log) error {
				return log.AppendSnapshot(9, 0, 0, 100, 1000, 0)
			},
		},
		{
			name: "standby snapshot",
			append: func(log *recordinglog.Log) error {
				return log.AppendStandbySnapshot(9, 0, 0, 100, 1000, 0, "aeron:udp?endpoint=standby:8020")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log, path := newLog(t)
			before, err := os.Stat(path)
			require.NoError(t, err)

			recordinglog.SetNextEntryIndexForTest(log, math.MaxInt32)
			err = tc.append(log)
			require.ErrorIs(t, err, recordinglog.ErrTooManyEntries)

			require.Empty(t, log.Entries())
			require.Equal(t, recordinglog.NullRecordingID, log.TermRecordingID())
			require.Nil(t, log.FindTermEntry(0))
			require.Equal(t, int32(math.MaxInt32), log.NextEntryIndex())

			after, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, before.Size(), after.Size())
		})
	}
}

func TestAppendTerm_duplicateRejected(t *testing.T) {
	t.Parallel()
	log, _ := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.ErrorIs(t, log.AppendTerm(9, 5, 100, 1000), recordinglog.ErrDuplicateTerm)
}

func TestAppendTerm_priorTermAutoCommit(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.NoError(t, log.AppendTerm(9, 6, 300, 2000))

	prior := findTerm(t, log.Entries(), 5)
	require.Equal(t, int64(300), prior.LogPosition)

	reloaded, err := recordinglog.Reload(path)
	require.NoError(t, err)
	require.Equal(t, int64(300), findTerm(t, reloaded, 5).LogPosition)
}

func TestAppend_cacheStaysInComparatorOrder(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.NoError(t, log.AppendSnapshot(9, 5, 100, 250, 1500, 0))
	require.NoError(t, log.AppendTerm(9, 3, 50, 900))
	require.NoError(t, log.AppendSnapshot(9, 3, 50, 80, 950, 0))

	entries := log.Entries()
	for i := 1; i < len(entries); i++ {
		require.LessOrEqual(t, recordinglog.CompareEntries(entries[i-1], entries[i]), 0)
	}
	requireReloadMatchesLog(t, path, log)
}

func TestAppend_shiftsInvalidSnapshotIndex(t *testing.T) {
	t.Parallel()
	invalidSnap := recordinglog.Entry{
		RecordingID:         9,
		LeadershipTermID:    10,
		TermBaseLogPosition: 100,
		LogPosition:         300,
		Timestamp:           2000,
		Type:                recordinglog.EntryTypeSnapshot,
		Valid:               false,
	}
	log, path := openWithEntries(t, termEntry(9), invalidSnap)

	require.Equal(t, []int32{1}, recordinglog.InvalidSnapshots(log))
	require.False(t, log.Entries()[1].Valid)

	// term 5 sorts ahead of term 10, so the invalid snapshot slot moves up one.
	require.NoError(t, log.AppendTerm(9, 5, 50, 900))

	shifted := recordinglog.InvalidSnapshots(log)
	require.Equal(t, []int32{2}, shifted)
	require.False(t, log.Entries()[shifted[0]].Valid)
	require.Equal(t, recordinglog.EntryTypeSnapshot, log.Entries()[shifted[0]].Type)

	requireReloadMatchesLog(t, path, log)
}

func TestAppendTerm_recordingIDValidation(t *testing.T) {
	t.Parallel()
	t.Run("null recordingId", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.ErrorIs(t, log.AppendTerm(-1, 5, 100, 1000), recordinglog.ErrTermRecordingIDInvalid)
	})
	t.Run("mismatched recordingId", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
		require.ErrorIs(t, log.AppendTerm(10, 6, 300, 2000), recordinglog.ErrTermRecordingIDMismatch)
	})
}

func TestAppendSnapshot_nullRecordingIDRejected(t *testing.T) {
	t.Parallel()
	log, _ := newLog(t)
	require.ErrorIs(
		t,
		log.AppendSnapshot(-1, 5, 100, 200, 1500, 0),
		recordinglog.ErrInvalidRecordingID,
	)
}

func TestAppendStandbySnapshot_endpointValidation(t *testing.T) {
	t.Parallel()
	t.Run("empty endpoint", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.ErrorIs(
			t,
			log.AppendStandbySnapshot(9, 5, 100, 200, 1500, 0, ""),
			recordinglog.ErrStandbyEndpointEmpty,
		)
	})
	t.Run("oversized endpoint", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		oversized := strings.Repeat("x", recordinglog.MaxEndpointLen+1)
		require.ErrorIs(
			t,
			log.AppendStandbySnapshot(9, 5, 100, 200, 1500, 0, oversized),
			recordinglog.ErrEndpointTooLong,
		)
	})
}

func TestAppend_writeErrorAfterClose(t *testing.T) {
	t.Parallel()
	t.Run("append path", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.NoError(t, log.Close())
		require.ErrorIs(t, log.AppendTerm(9, 5, 100, 1000), os.ErrClosed)
	})
	t.Run("failed append does not store recordingId", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.NoError(t, log.Close())
		require.ErrorIs(t, log.AppendTerm(9, 5, 100, 1000), os.ErrClosed)
		err := log.AppendTerm(10, 5, 100, 1000)
		require.ErrorIs(t, err, os.ErrClosed)
		require.NotErrorIs(t, err, recordinglog.ErrTermRecordingIDMismatch)
	})
	t.Run("auto-commit field write", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
		require.NoError(t, log.Close())
		// AppendTerm(6) commits term 5 in place before appending; the field
		// write fails first on the closed handle.
		require.ErrorIs(t, log.AppendTerm(9, 6, 300, 2000), os.ErrClosed)
	})
}

func TestFindLastTerm(t *testing.T) {
	t.Parallel()
	t.Run("empty log", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.Nil(t, recordinglog.FindLastTerm(log))
	})
	t.Run("newest valid term", func(t *testing.T) {
		t.Parallel()
		log, _ := newLog(t)
		require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
		require.NoError(t, log.AppendTerm(9, 6, 300, 2000))
		require.NoError(t, log.AppendSnapshot(9, 6, 300, 400, 2100, 0))

		last := recordinglog.FindLastTerm(log)
		require.NotNil(t, last)
		require.Equal(t, int64(6), last.LeadershipTermID)
	})
}

func TestCommitLogPosition_writesEightBytesAtOffset(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))

	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, log.CommitLogPosition(5, 555))
	after, err := os.ReadFile(path)
	require.NoError(t, err)

	term := findTerm(t, log.Entries(), 5)
	fieldStart := int(term.Position) + recordinglog.LogPositionOffset

	start, end := diffRange(before, after)
	require.Equal(t, fieldStart, start)
	require.Equal(t, fieldStart+8, end)
	require.Equal(t, int64(555), int64(binary.LittleEndian.Uint64(after[start:end])))

	reloaded, err := recordinglog.Reload(path)
	require.NoError(t, err)
	require.Equal(t, int64(555), findTerm(t, reloaded, 5).LogPosition)
}

func TestCommitLogPosition_unknownTerm(t *testing.T) {
	t.Parallel()
	log, _ := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.ErrorIs(t, log.CommitLogPosition(6, 555), recordinglog.ErrUnknownLeadershipTermID)
}

func TestInvalidateEntry_writesFourBytesWithHighBit(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.NoError(t, log.AppendSnapshot(9, 5, 100, 200, 1500, 0))

	snapshotPosition := log.Entries()[1].Position
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, recordinglog.InvalidateEntry(log, 1))
	after, err := os.ReadFile(path)
	require.NoError(t, err)

	fieldStart := int(snapshotPosition) + recordinglog.EntryTypeOffset
	start, end := diffRange(before, after)
	require.GreaterOrEqual(t, start, fieldStart, "no bytes before the type word change")
	require.LessOrEqual(t, end, fieldStart+4, "no bytes after the type word change")

	typeWord := int32(binary.LittleEndian.Uint32(after[fieldStart : fieldStart+4]))
	require.Negative(t, typeWord, "invalid flag is the high bit")
	require.Equal(t, recordinglog.EntryTypeSnapshot, typeWord&0x7fffffff)

	reloaded, err := recordinglog.Reload(path)
	require.NoError(t, err)
	require.False(t, reloaded[1].Valid)
}

func TestInvalidateEntry_termDropsIndexMapEntry(t *testing.T) {
	t.Parallel()
	log, _ := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))

	require.NoError(t, recordinglog.InvalidateEntry(log, 0))
	require.ErrorIs(t, log.CommitLogPosition(5, 555), recordinglog.ErrUnknownLeadershipTermID)
	require.False(t, log.Entries()[0].Valid)
}

func TestInvalidateLatestSnapshot_multiServiceGroup(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.NoError(t, log.AppendSnapshot(9, 5, 100, 200, 1500, -1))
	require.NoError(t, log.AppendSnapshot(9, 5, 100, 200, 1500, 0))
	require.NoError(t, log.AppendSnapshot(9, 5, 100, 200, 1500, 1))

	found, err := log.InvalidateLatestSnapshot()
	require.NoError(t, err)
	require.True(t, found)

	for _, e := range log.Entries() {
		if e.Type == recordinglog.EntryTypeTerm {
			require.True(t, e.Valid)
			continue
		}
		require.False(t, e.Valid)
	}
	require.Len(t, recordinglog.InvalidSnapshots(log), 3)

	requireReloadMatchesLog(t, path, log)
}

func TestInvalidateLatestSnapshot_noMatchingTermNoPartial(t *testing.T) {
	t.Parallel()
	log, _ := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.NoError(t, log.AppendSnapshot(9, 99, 100, 200, 1500, -1))

	found, err := log.InvalidateLatestSnapshot()
	require.ErrorIs(t, err, recordinglog.ErrNoMatchingTermForSnapshot)
	require.False(t, found)

	require.True(t, log.Entries()[1].Valid)
	require.Empty(t, recordinglog.InvalidSnapshots(log))
}

func TestInvalidateLatestSnapshot_missingTermInMaxGroupNoPartial(t *testing.T) {
	t.Parallel()
	log, _ := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.NoError(t, log.AppendSnapshot(9, 5, 100, 200, 1500, -1))
	// term 3 is never appended; its consensus snapshot shares the max
	// logPosition with term 5's, so it is collected but not at indices[0].
	require.NoError(t, log.AppendSnapshot(9, 3, 50, 200, 1500, -1))

	found, err := log.InvalidateLatestSnapshot()
	require.ErrorIs(t, err, recordinglog.ErrNoMatchingTermForSnapshot)
	require.False(t, found)

	for _, e := range log.Entries() {
		require.True(t, e.Valid)
	}
	require.Empty(t, recordinglog.InvalidSnapshots(log))
}

func TestInvalidateLatestSnapshot_ignoresStandbyOutsideScanWindow(t *testing.T) {
	t.Parallel()

	const ts = int64(1_000_000_000)
	const endpoint = "remotehost0.aeron.io:20002"

	log, path := newLog(t)
	require.NoError(t, log.AppendTerm(1, 0, 0, ts))
	require.NoError(t, log.AppendSnapshot(2, 0, 0, 1000, ts, -1))
	require.NoError(t, log.AppendSnapshot(3, 0, 0, 1000, ts, 0))
	require.NoError(t, log.AppendSnapshot(4, 0, 2, 2000, ts, -1))
	require.NoError(t, log.AppendSnapshot(5, 0, 2, 2000, ts, 0))
	require.NoError(t, log.AppendStandbySnapshot(15, 0, 500, 800, ts, -1, endpoint))
	require.NoError(t, log.AppendStandbySnapshot(16, 0, 500, 800, ts, 0, endpoint))

	latestServiceSnapshot := func() int64 {
		for _, e := range slices.Backward(log.Entries()) {
			if e.Valid && e.Type == recordinglog.EntryTypeSnapshot && e.ServiceID == -1 {
				return e.RecordingID
			}
		}
		return 0
	}

	require.Equal(t, int64(4), latestServiceSnapshot())

	found, err := log.InvalidateLatestSnapshot()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), latestServiceSnapshot())

	for _, e := range log.Entries() {
		switch e.RecordingID {
		case 1:
			require.True(t, e.Valid)
		case 2, 3, 15, 16:
			require.True(t, e.Valid, "recordingId %d", e.RecordingID)
		case 4, 5:
			require.False(t, e.Valid, "recordingId %d", e.RecordingID)
		}
	}

	requireReloadMatchesLog(t, path, log)
}

func TestInvalidate_thenAppendBeforeKeepsSlot(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	require.NoError(t, log.AppendTerm(9, 10, 100, 1000))
	require.NoError(t, log.AppendSnapshot(9, 10, 100, 300, 2000, -1))

	found, err := log.InvalidateLatestSnapshot()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []int32{1}, recordinglog.InvalidSnapshots(log))

	// term 5 sorts before term 10, so the invalid snapshot slot shifts up one.
	require.NoError(t, log.AppendTerm(9, 5, 50, 900))

	shifted := recordinglog.InvalidSnapshots(log)
	require.Equal(t, []int32{2}, shifted)
	slot := log.Entries()[shifted[0]]
	require.False(t, slot.Valid)
	require.Equal(t, recordinglog.EntryTypeSnapshot, slot.Type)
	require.Equal(t, int64(10), slot.LeadershipTermID)

	requireReloadMatchesLog(t, path, log)
}

func TestInvalidateLatestSnapshot_reSnapshotRestoresInPlace(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))
	require.NoError(t, log.AppendSnapshot(9, 5, 100, 200, 1500, -1))

	found, err := log.InvalidateLatestSnapshot()
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, recordinglog.InvalidSnapshots(log), 1)

	entriesBefore := len(log.Entries())
	nextBefore := log.NextEntryIndex()

	require.NoError(t, log.AppendSnapshot(7, 5, 100, 200, 1600, -1))

	require.Len(t, log.Entries(), entriesBefore)
	require.Equal(t, nextBefore, log.NextEntryIndex())
	require.Empty(t, recordinglog.InvalidSnapshots(log))

	restored := log.Entries()[1]
	require.True(t, restored.Valid)
	require.Equal(t, int64(7), restored.RecordingID)
	require.Equal(t, int64(1600), restored.Timestamp)

	requireReloadMatchesLog(t, path, log)
}

func TestRestoreInvalidSnapshot_differingTermBaseAppendsFresh(t *testing.T) {
	t.Parallel()
	// term, an invalid standby (termBase 100, short endpoint), and a valid
	// standby neighbour (termBase 500). A re-append with the same identity but a
	// higher termBase and a longer endpoint must not restore the invalid slot in
	// place: termBase is a comparator sort key, and a longer endpoint would
	// overrun the contiguous neighbour. The fresh append must reproduce a clean
	// Reload.
	term := recordinglog.Entry{
		RecordingID:         9,
		LeadershipTermID:    5,
		TermBaseLogPosition: 100,
		LogPosition:         200,
		Timestamp:           1000,
		Type:                recordinglog.EntryTypeTerm,
		Valid:               true,
	}
	invalidLow := recordinglog.Entry{
		RecordingID:         9,
		LeadershipTermID:    5,
		TermBaseLogPosition: 100,
		LogPosition:         300,
		Timestamp:           1500,
		ServiceID:           -1,
		Type:                recordinglog.EntryTypeStandbySnapshot,
		ArchiveEndpoint:     "ep-low",
	}
	validHigh := recordinglog.Entry{
		RecordingID:         9,
		LeadershipTermID:    5,
		TermBaseLogPosition: 500,
		LogPosition:         300,
		Timestamp:           1500,
		ServiceID:           0,
		Type:                recordinglog.EntryTypeStandbySnapshot,
		Valid:               true,
		ArchiveEndpoint:     "ep-high",
	}
	log, path := openWithEntries(t, term, invalidLow, validHigh)

	entriesBefore := len(log.Entries())
	require.NoError(
		t,
		log.AppendStandbySnapshot(9, 5, 999, 300, 1600, -1, "ep-much-longer-endpoint-value"),
	)

	require.Len(t, log.Entries(), entriesBefore+1)
	require.Len(t, recordinglog.InvalidSnapshots(log), 1)

	requireReloadMatchesLog(t, path, log)
}

func TestRestoreInvalidSnapshot_longerEndpointReusesExistingSpan(t *testing.T) {
	t.Parallel()
	// term, an invalidated standby with a short endpoint, then a snapshot
	// neighbour contiguous on disk. A re-append with the same match key but a
	// longer endpoint restores the slot in place. The restored record must reuse
	// the existing short endpoint so its span never grows: a longer encode would
	// overrun the neighbour record that follows it.
	term := recordinglog.Entry{
		RecordingID:         9,
		LeadershipTermID:    5,
		TermBaseLogPosition: 100,
		LogPosition:         200,
		Timestamp:           1000,
		Type:                recordinglog.EntryTypeTerm,
		Valid:               true,
	}
	invalidStandby := recordinglog.Entry{
		RecordingID:         9,
		LeadershipTermID:    5,
		TermBaseLogPosition: 100,
		LogPosition:         300,
		Timestamp:           1500,
		ServiceID:           -1,
		Type:                recordinglog.EntryTypeStandbySnapshot,
		ArchiveEndpoint:     "ep",
	}
	neighbour := recordinglog.Entry{
		RecordingID:         9,
		LeadershipTermID:    5,
		TermBaseLogPosition: 100,
		LogPosition:         400,
		Timestamp:           1700,
		ServiceID:           0,
		Type:                recordinglog.EntryTypeSnapshot,
		Valid:               true,
	}
	log, path := openWithEntries(t, term, invalidStandby, neighbour)

	invalidSlots := recordinglog.InvalidSnapshots(log)
	require.Len(t, invalidSlots, 1)
	slot := invalidSlots[0]
	slotPosition := log.Entries()[slot].Position
	neighbourPosition := nextRecordPosition(t, log.Entries(), slotPosition)

	entriesBefore := len(log.Entries())
	nextBefore := log.NextEntryIndex()
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	// Longer endpoint than the stored "ep": its encode would cross the next
	// 64-byte record boundary and overrun the neighbour if the span grew.
	require.NoError(
		t,
		log.AppendStandbySnapshot(9, 5, 100, 300, 1600, -1, "ep-much-longer-endpoint-value"),
	)

	// Restore fired in place: no fresh entry, slot re-validated, slot dropped
	// from the invalid set.
	require.Len(t, log.Entries(), entriesBefore)
	require.Equal(t, nextBefore, log.NextEntryIndex())
	require.Empty(t, recordinglog.InvalidSnapshots(log))

	restored := log.Entries()[slot]
	require.True(t, restored.Valid)
	require.Equal(t, "ep", restored.ArchiveEndpoint)
	require.Equal(t, int64(1600), restored.Timestamp)

	// The disk write stays within the slot's own span: it never reaches the
	// neighbour record that begins at neighbourPosition.
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	start, end := diffRange(before, after)
	require.GreaterOrEqual(t, start, int(slotPosition), "write started before the slot")
	require.LessOrEqual(t, end, int(neighbourPosition), "write overran into the neighbour record")

	requireReloadMatchesLog(t, path, log)
}

func TestWriteFieldAt_zeroAlloc(t *testing.T) {
	log, _ := newLog(t)
	require.NoError(t, log.AppendTerm(9, 5, 100, 1000))

	var field [8]byte
	atomicbuffer.Wrap(field[:]).PutInt64(0, 555)
	testutil.AssertZeroAllocs(t, "writeFieldAt", 100, func() {
		_ = recordinglog.WriteFieldAt(log, 0, recordinglog.LogPositionOffset, field[:])
	})
}

const ensureCoherentRecordingID = 9234236

var ensureCoherentCases = []struct {
	name        string
	initialTerm int64
	initialBase int64
	targetTerm  int64
}{
	{"from zero", 0, 0, 4},
	{"from two", 2, 500, 4},
	{"large gap", 100, 1000000, 200},
}

func TestEnsureCoherent_emptyLogCreatesInitialTerms(t *testing.T) {
	t.Parallel()
	for _, tc := range ensureCoherentCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log, path := newLog(t)
			termBase := tc.initialBase + 100
			logPosition := tc.initialBase + 500
			const timestamp = 10000

			require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
				RecordingID:                ensureCoherentRecordingID,
				InitialLogLeadershipTermID: tc.initialTerm,
				InitialTermBaseLogPosition: tc.initialBase,
				LeadershipTermID:           tc.targetTerm,
				TermBaseLogPosition:        termBase,
				LogPosition:                logPosition,
				Timestamp:                  timestamp,
				FileSyncLevel:              recordinglog.FileSyncNone,
			}))

			lowest := max(int64(0), tc.initialTerm)
			require.Len(t, log.Entries(), int(tc.targetTerm-lowest+1))

			// The final loop append commits the prior term (targetTerm-1) to
			// termBase, so only the terms before it carry initialBase.
			for termID := lowest; termID < tc.targetTerm; termID++ {
				entry := findTerm(t, log.Entries(), termID)
				require.Equal(t, tc.initialBase, entry.TermBaseLogPosition)
				wantLogPosition := tc.initialBase
				if termID == tc.targetTerm-1 {
					wantLogPosition = termBase
				}
				require.Equal(t, wantLogPosition, entry.LogPosition)
				require.Equal(t, int64(timestamp), entry.Timestamp)
			}

			target := findTerm(t, log.Entries(), tc.targetTerm)
			require.Equal(t, termBase, target.TermBaseLogPosition)
			require.Equal(t, logPosition, target.LogPosition)
			require.Equal(t, int64(timestamp), target.Timestamp)

			requireReloadMatchesLog(t, path, log)
		})
	}
}

func TestEnsureCoherent_skipsMinusOneInitialTermId(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
		RecordingID:                ensureCoherentRecordingID,
		InitialLogLeadershipTermID: -1,
		InitialTermBaseLogPosition: 0,
		LeadershipTermID:           4,
		TermBaseLogPosition:        0,
		LogPosition:                500,
		Timestamp:                  1000,
		FileSyncLevel:              recordinglog.FileSyncNone,
	}))
	for _, e := range log.Entries() {
		require.NotEqual(t, int64(-1), e.LeadershipTermID)
	}
	requireReloadMatchesLog(t, path, log)
}

func TestEnsureCoherent_backfillsGapInNonEmptyLog(t *testing.T) {
	t.Parallel()
	for _, tc := range ensureCoherentCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log, path := newLog(t)
			termBase := tc.initialBase + 100
			logPosition := tc.initialBase + 500
			const nowNs = 1000000

			require.NoError(t,
				log.AppendTerm(ensureCoherentRecordingID, tc.initialTerm, tc.initialBase, nowNs))

			require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
				RecordingID:                ensureCoherentRecordingID,
				InitialLogLeadershipTermID: tc.initialTerm,
				InitialTermBaseLogPosition: tc.initialBase,
				LeadershipTermID:           tc.targetTerm,
				TermBaseLogPosition:        termBase,
				LogPosition:                logPosition,
				Timestamp:                  nowNs,
				FileSyncLevel:              recordinglog.FileSyncNone,
			}))

			for termID := tc.initialTerm + 1; termID < tc.targetTerm; termID++ {
				entry := findTerm(t, log.Entries(), termID)
				require.Equal(t, termBase, entry.TermBaseLogPosition)
				require.Equal(t, termBase, entry.LogPosition)
			}

			target := findTerm(t, log.Entries(), tc.targetTerm)
			require.Equal(t, termBase, target.TermBaseLogPosition)
			require.Equal(t, logPosition, target.LogPosition)

			requireReloadMatchesLog(t, path, log)
		})
	}
}

func TestEnsureCoherent_backfillsGapWhenHigherTermExists(t *testing.T) {
	t.Parallel()
	const (
		termBase    = 100
		logPosition = 500
		nowNs       = 1000000
	)

	t.Run("commits target", func(t *testing.T) {
		t.Parallel()
		log, path := newLog(t)

		require.NoError(t, log.AppendTerm(ensureCoherentRecordingID, 1, termBase, nowNs))
		require.NoError(t, log.CommitLogPosition(1, termBase))
		require.NoError(t, log.AppendTerm(ensureCoherentRecordingID, 5, 700, nowNs))

		require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
			RecordingID:                ensureCoherentRecordingID,
			InitialLogLeadershipTermID: 0,
			InitialTermBaseLogPosition: 0,
			LeadershipTermID:           3,
			TermBaseLogPosition:        termBase,
			LogPosition:                logPosition,
			Timestamp:                  nowNs,
			FileSyncLevel:              recordinglog.FileSyncNone,
		}))

		for termID := int64(2); termID < 3; termID++ {
			entry := findTerm(t, log.Entries(), termID)
			require.Equal(t, int64(termBase), entry.TermBaseLogPosition)
			require.Equal(t, int64(termBase), entry.LogPosition)
		}

		target := findTerm(t, log.Entries(), 3)
		require.Equal(t, int64(termBase), target.TermBaseLogPosition)
		require.Equal(t, int64(logPosition), target.LogPosition)

		requireReloadMatchesLog(t, path, log)
	})

	t.Run("leaves target uncommitted", func(t *testing.T) {
		t.Parallel()
		log, path := newLog(t)

		require.NoError(t, log.AppendTerm(ensureCoherentRecordingID, 1, termBase, nowNs))
		require.NoError(t, log.CommitLogPosition(1, termBase))
		require.NoError(t, log.AppendTerm(ensureCoherentRecordingID, 5, 700, nowNs))

		require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
			RecordingID:                ensureCoherentRecordingID,
			InitialLogLeadershipTermID: 0,
			InitialTermBaseLogPosition: 0,
			LeadershipTermID:           3,
			TermBaseLogPosition:        termBase,
			LogPosition:                recordinglog.NullPosition,
			Timestamp:                  nowNs,
			FileSyncLevel:              recordinglog.FileSyncNone,
		}))

		target := findTerm(t, log.Entries(), 3)
		require.Equal(t, int64(termBase), target.TermBaseLogPosition)
		require.Equal(t, recordinglog.NullPosition, target.LogPosition)

		requireReloadMatchesLog(t, path, log)
	})
}

func TestEnsureCoherent_completesExistingTerm(t *testing.T) {
	t.Parallel()
	for _, tc := range ensureCoherentCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log, path := newLog(t)
			termBase := tc.initialBase + 100
			logPosition := tc.initialBase + 500
			const nowNs = 1000000

			require.NoError(t, log.AppendTerm(ensureCoherentRecordingID, tc.targetTerm, termBase, nowNs))

			require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
				RecordingID:                ensureCoherentRecordingID,
				InitialLogLeadershipTermID: tc.initialTerm,
				InitialTermBaseLogPosition: tc.initialBase,
				LeadershipTermID:           tc.targetTerm,
				TermBaseLogPosition:        termBase,
				LogPosition:                logPosition,
				Timestamp:                  nowNs,
				FileSyncLevel:              recordinglog.FileSyncNone,
			}))

			target := findTerm(t, log.Entries(), tc.targetTerm)
			require.Equal(t, termBase, target.TermBaseLogPosition)
			require.Equal(t, logPosition, target.LogPosition)
			require.Len(t, log.Entries(), 1)

			requireReloadMatchesLog(t, path, log)
		})
	}
}

func TestEnsureCoherent_backfillsPriorTerm(t *testing.T) {
	t.Parallel()
	log, path := newLog(t)
	const (
		termBase = 10000
		nowNs    = 1000000
	)

	require.NoError(t, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
		RecordingID:                ensureCoherentRecordingID,
		InitialLogLeadershipTermID: 0,
		InitialTermBaseLogPosition: 0,
		LeadershipTermID:           1,
		TermBaseLogPosition:        termBase,
		LogPosition:                recordinglog.NullPosition,
		Timestamp:                  nowNs,
		FileSyncLevel:              recordinglog.FileSyncNone,
	}))

	term0 := findTerm(t, log.Entries(), 0)
	require.Equal(t, int64(0), term0.TermBaseLogPosition)
	require.Equal(t, int64(termBase), term0.LogPosition)

	term1 := findTerm(t, log.Entries(), 1)
	require.Equal(t, int64(termBase), term1.TermBaseLogPosition)
	require.Equal(t, recordinglog.NullPosition, term1.LogPosition)

	require.Len(t, log.Entries(), 2)
	requireReloadMatchesLog(t, path, log)
}

func TestEnsureCoherent_priorTermUncommittedNoBaseReturnsError(t *testing.T) {
	t.Parallel()
	log, _ := newLog(t)
	const (
		leadershipTermID = 4
		nowNs            = 1000000
	)

	require.NoError(t, log.AppendTerm(ensureCoherentRecordingID, leadershipTermID-1, 0, nowNs))

	err := log.EnsureCoherent(recordinglog.EnsureCoherentParams{
		RecordingID:                ensureCoherentRecordingID,
		InitialLogLeadershipTermID: 0,
		InitialTermBaseLogPosition: 0,
		LeadershipTermID:           leadershipTermID,
		TermBaseLogPosition:        recordinglog.NullPosition,
		LogPosition:                500,
		Timestamp:                  nowNs,
		FileSyncLevel:              recordinglog.FileSyncData,
	})
	require.ErrorIs(t, err, recordinglog.ErrPriorTermNotCommitted)

	require.Len(t, log.Entries(), 1)
	priorTerm := findTerm(t, log.Entries(), leadershipTermID-1)
	require.Equal(t, recordinglog.NullPosition, priorTerm.LogPosition)
}

func TestEnsureCoherent_writeErrorPropagates(t *testing.T) {
	t.Parallel()
	const recordingID = ensureCoherentRecordingID

	for _, tc := range []struct {
		name string
		seed func(*recordinglog.Log)
		call func(*recordinglog.Log) error
	}{
		{
			name: "empty log append",
			call: func(log *recordinglog.Log) error {
				return log.EnsureCoherent(recordinglog.EnsureCoherentParams{
					RecordingID:                recordingID,
					InitialLogLeadershipTermID: 0,
					InitialTermBaseLogPosition: 0,
					LeadershipTermID:           3,
					TermBaseLogPosition:        0,
					LogPosition:                recordinglog.NullPosition,
					Timestamp:                  1000,
					FileSyncLevel:              recordinglog.FileSyncData,
				})
			},
		},
		{
			name: "prior term commit",
			seed: func(log *recordinglog.Log) {
				require.NoError(t, log.AppendTerm(recordingID, 2, 100, 1000))
			},
			call: func(log *recordinglog.Log) error {
				return log.EnsureCoherent(recordinglog.EnsureCoherentParams{
					RecordingID:                recordingID,
					InitialLogLeadershipTermID: 0,
					InitialTermBaseLogPosition: 0,
					LeadershipTermID:           4,
					TermBaseLogPosition:        200,
					LogPosition:                600,
					Timestamp:                  1000,
					FileSyncLevel:              recordinglog.FileSyncData,
				})
			},
		},
		{
			name: "backfill append",
			seed: func(log *recordinglog.Log) {
				require.NoError(t, log.AppendTerm(recordingID, 2, 100, 1000))
				require.NoError(t, log.AppendTerm(recordingID, 3, 200, 1000))
				require.NoError(t, log.CommitLogPosition(3, 300))
			},
			call: func(log *recordinglog.Log) error {
				return log.EnsureCoherent(recordinglog.EnsureCoherentParams{
					RecordingID:                recordingID,
					InitialLogLeadershipTermID: 0,
					InitialTermBaseLogPosition: 0,
					LeadershipTermID:           5,
					TermBaseLogPosition:        0,
					LogPosition:                800,
					Timestamp:                  1000,
					FileSyncLevel:              recordinglog.FileSyncData,
				})
			},
		},
		{
			name: "target commit",
			seed: func(log *recordinglog.Log) {
				require.NoError(t, log.AppendTerm(recordingID, 3, 100, 1000))
			},
			call: func(log *recordinglog.Log) error {
				return log.EnsureCoherent(recordinglog.EnsureCoherentParams{
					RecordingID:                recordingID,
					InitialLogLeadershipTermID: 0,
					InitialTermBaseLogPosition: 0,
					LeadershipTermID:           3,
					TermBaseLogPosition:        100,
					LogPosition:                500,
					Timestamp:                  1000,
					FileSyncLevel:              recordinglog.FileSyncData,
				})
			},
		},
		{
			name: "force",
			seed: func(log *recordinglog.Log) {
				require.NoError(t, log.AppendTerm(recordingID, 3, 100, 1000))
			},
			call: func(log *recordinglog.Log) error {
				return log.EnsureCoherent(recordinglog.EnsureCoherentParams{
					RecordingID:                recordingID,
					InitialLogLeadershipTermID: 0,
					InitialTermBaseLogPosition: 0,
					LeadershipTermID:           3,
					TermBaseLogPosition:        100,
					LogPosition:                recordinglog.NullPosition,
					Timestamp:                  1000,
					FileSyncLevel:              recordinglog.FileSyncData,
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log, _ := newLog(t)
			if tc.seed != nil {
				tc.seed(log)
			}
			require.NoError(t, log.Close())
			require.Error(t, tc.call(log))
		})
	}
}

// diffRange returns the half-open byte range [start, end) where a and b differ.
// It backs the in-place mutation goldens that assert exactly which bytes a
// single-field write touches. Equal slices return (len, len).
func diffRange(a, b []byte) (int, int) {
	start := 0
	for start < len(a) && start < len(b) && a[start] == b[start] {
		start++
	}
	end := len(b)
	for end > start && a[end-1] == b[end-1] {
		end--
	}
	return start, end
}

// nextRecordPosition returns the on-disk start offset of the record that
// immediately follows the one at position: the smallest entry Position greater
// than it. It backs the overrun guard that asserts an in-place rewrite never
// crosses into the next contiguous record.
func nextRecordPosition(
	t *testing.T,
	entries []recordinglog.Entry,
	position int64,
) int {
	t.Helper()
	next := int64(-1)
	for _, e := range entries {
		if e.Position > position && (next == -1 || e.Position < next) {
			next = e.Position
		}
	}
	require.NotEqual(t, int64(-1), next, "no record follows position %d", position)
	return int(next)
}

func findTerm(
	t *testing.T,
	entries []recordinglog.Entry,
	leadershipTermID int64,
) recordinglog.Entry {
	t.Helper()
	for _, e := range entries {
		if e.Type == recordinglog.EntryTypeTerm && e.LeadershipTermID == leadershipTermID {
			return e
		}
	}
	t.Fatalf("term %d not found", leadershipTermID)
	return recordinglog.Entry{}
}

// openFDCount counts process file descriptors that resolve to path. It backs the
// fd-leak assertion on Open's decode-error cleanup branch and skips where the
// /proc fd table is unavailable.
func openFDCount(t *testing.T, path string) int {
	t.Helper()
	target, err := filepath.EvalSymlinks(path)
	if err != nil {
		target = path
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot read /proc/self/fd: %v", err)
	}
	count := 0
	for _, fd := range fds {
		link, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err != nil {
			continue
		}
		if link == target || link == path {
			count++
		}
	}
	return count
}

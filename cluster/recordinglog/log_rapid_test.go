//go:build rapid

package recordinglog_test

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func requireReloadMatchesLogRapid(rt *rapid.T, path string, log *recordinglog.Log) {
	rt.Helper()
	reloaded, err := recordinglog.Reload(path)
	require.NoError(rt, err)
	entries := log.Entries()
	if entries == nil {
		entries = []recordinglog.Entry{}
	}
	require.Equal(rt, reloaded, entries)
}

// writeLogFile draws a random sequence of encoded entries interleaved with
// NULL_VALUE removed slots, writes it under dir, and returns the entry ordinal
// (entries plus null slots, the value Open must seed nextEntryIndex to).
func writeLogFile(rt *rapid.T, dir string) int32 {
	rt.Helper()
	n := rapid.IntRange(0, 8).Draw(rt, "n")
	termRecordingID := int64(-1)
	var buf []byte
	slots := int32(0)
	for i := range n {
		if rapid.Bool().Draw(rt, "null"+strconv.Itoa(i)) {
			buf = append(buf, nullSlotBuf(recordinglog.RecordAlignment)...)
			slots++
			continue
		}
		e, updated := recordinglog.DrawEntry(rt, strconv.Itoa(i), termRecordingID)
		termRecordingID = updated
		buf = append(buf, recordinglog.EncodeEntry(e)...)
		slots++
	}
	require.NoError(rt, os.WriteFile(filepath.Join(dir, "recording.log"), buf, 0o600))
	return slots
}

type appendDrawProfile int

const (
	randomAppendDraw appendDrawProfile = iota
	stableRestoreAppendDraw
)

// drawAppend applies one random append to log, honouring the validators:
// TERM ids stay unique with one shared recordingID, snapshot recordingIDs are
// non-null, and standby endpoints are non-empty. usedTerms tracks claimed TERM
// ids; termRecordingID is the shared TERM recordingID (-1 until the first TERM).
//
// stableRestoreAppendDraw fixes termBase and standby endpoint length per snapshot
// identity so a later restoreInvalidSnapshot stays byte-for-byte and order-stable.
func drawAppend(
	rt *rapid.T,
	name string,
	log *recordinglog.Log,
	usedTerms map[int64]bool,
	termRecordingID int64,
	profile appendDrawProfile,
) int64 {
	rt.Helper()
	kind := rapid.SampledFrom([]int32{
		recordinglog.EntryTypeTerm,
		recordinglog.EntryTypeSnapshot,
		recordinglog.EntryTypeStandbySnapshot,
	}).Draw(rt, name+"_kind")

	var leadershipTermID int64
	var termBase int64
	var logPosition int64
	var serviceID int32
	switch profile {
	case stableRestoreAppendDraw:
		leadershipTermID = rapid.Int64Range(0, 6).Draw(rt, name+"_leadershipTermID")
		termBase = leadershipTermID * 64
		logPosition = rapid.Int64Range(0, 4).Draw(rt, name+"_logPosition")
		serviceID = rapid.Int32Range(-1, 2).Draw(rt, name+"_serviceID")
	default:
		leadershipTermID = rapid.Int64Range(0, 12).Draw(rt, name+"_leadershipTermID")
		termBase = rapid.Int64Range(0, 1000).Draw(rt, name+"_termBase")
		logPosition = rapid.Int64Range(0, 1000).Draw(rt, name+"_logPosition")
		serviceID = rapid.Int32Range(-1, 4).Draw(rt, name+"_serviceID")
	}
	timestamp := rapid.Int64Range(0, 999999).Draw(rt, name+"_timestamp")

	switch kind {
	case recordinglog.EntryTypeTerm:
		recordingID := rapid.Int64Range(0, 1000).Draw(rt, name+"_recordingID")
		if termRecordingID != -1 {
			recordingID = termRecordingID
		}
		err := log.AppendTerm(recordingID, leadershipTermID, termBase, timestamp)
		if usedTerms[leadershipTermID] {
			require.ErrorIs(rt, err, recordinglog.ErrDuplicateTerm)
			return termRecordingID
		}
		require.NoError(rt, err)
		usedTerms[leadershipTermID] = true
		return recordingID
	case recordinglog.EntryTypeStandbySnapshot:
		recordingID := rapid.Int64Range(0, 1000).Draw(rt, name+"_recordingID")
		var endpoint string
		if profile == stableRestoreAppendDraw {
			endpoint = "ep" + strconv.FormatInt(leadershipTermID, 10) + "-" + strconv.Itoa(int(serviceID))
		} else {
			endpoint = string(rapid.SliceOfN(rapid.ByteRange('a', 'z'), 1, 20).Draw(rt, name+"_ep"))
		}
		require.NoError(rt, log.AppendStandbySnapshot(
			recordingID, leadershipTermID, termBase, logPosition, timestamp, serviceID, endpoint,
		))
		return termRecordingID
	default:
		recordingID := rapid.Int64Range(0, 1000).Draw(rt, name+"_recordingID")
		require.NoError(rt, log.AppendSnapshot(
			recordingID, leadershipTermID, termBase, logPosition, timestamp, serviceID,
		))
		return termRecordingID
	}
}

func TestAppendInsertionOrder_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		log, path := newLog(t)
		defer func() { require.NoError(rt, log.Close()) }()

		usedTerms := make(map[int64]bool)
		termRecordingID := int64(-1)
		n := rapid.IntRange(0, 10).Draw(rt, "n")
		for i := range n {
			termRecordingID = drawAppend(
				rt, strconv.Itoa(i), log, usedTerms, termRecordingID, randomAppendDraw,
			)

			entries := log.Entries()
			for j := 1; j < len(entries); j++ {
				require.LessOrEqual(rt, recordinglog.CompareEntries(entries[j-1], entries[j]), 0)
			}
			requireReloadMatchesLogRapid(rt, path, log)
		}
	})
}

func TestTermRecordingID_matchesEveryTermEntry_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()
		log, _ := newLog(t)
		defer func() { require.NoError(rt, log.Close()) }()

		usedTerms := make(map[int64]bool)
		termRecordingID := int64(-1)
		n := rapid.IntRange(0, 12).Draw(rt, "n")
		for i := range n {
			termRecordingID = drawAppend(
				rt, strconv.Itoa(i), log, usedTerms, termRecordingID, randomAppendDraw,
			)
			shared := log.TermRecordingID()
			if termRecordingID == -1 {
				require.Equal(rt, recordinglog.NullRecordingID, shared)
			} else {
				require.Equal(rt, termRecordingID, shared)
			}
			for _, e := range log.Entries() {
				if e.Type != recordinglog.EntryTypeTerm || !e.Valid {
					continue
				}
				require.Equal(rt, shared, e.RecordingID)
			}
		}
	})
}

func TestTermRecordingID_survivesCloseReopen_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()
		dir := t.TempDir()
		log, err := recordinglog.Open(dir, true)
		require.NoError(rt, err)
		defer func() { require.NoError(rt, log.Close()) }()

		usedTerms := make(map[int64]bool)
		termRecordingID := int64(-1)
		n := rapid.IntRange(0, 10).Draw(rt, "n")
		for i := range n {
			termRecordingID = drawAppend(
				rt, strconv.Itoa(i), log, usedTerms, termRecordingID, randomAppendDraw,
			)
		}
		before := log.TermRecordingID()
		require.NoError(rt, log.Close())

		reopened, err := recordinglog.Open(dir, false)
		require.NoError(rt, err)
		defer func() { require.NoError(rt, reopened.Close()) }()
		require.Equal(rt, before, reopened.TermRecordingID())
		if termRecordingID == -1 {
			require.Equal(rt, recordinglog.NullRecordingID, reopened.TermRecordingID())
		} else {
			require.Equal(rt, termRecordingID, reopened.TermRecordingID())
		}
	})
}

func TestMutateThenReload_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		log, path := newLog(t)
		defer func() { require.NoError(rt, log.Close()) }()
		usedTerms := make(map[int64]bool)
		termRecordingID := int64(-1)
		n := rapid.IntRange(0, 14).Draw(rt, "n")
		ops := []string{"append", "commit", "invalidate"}
		for i := range n {
			op := rapid.SampledFrom(ops).Draw(rt, "op"+strconv.Itoa(i))
			switch op {
			case "commit":
				termID := rapid.Int64Range(0, 6).Draw(rt, "commitTerm"+strconv.Itoa(i))
				pos := rapid.Int64Range(0, 1000).Draw(rt, "commitPos"+strconv.Itoa(i))
				err := log.CommitLogPosition(termID, pos)
				if usedTerms[termID] {
					require.NoError(rt, err)
				} else {
					require.ErrorIs(rt, err, recordinglog.ErrUnknownLeadershipTermID)
				}
			case "invalidate":
				if _, err := log.InvalidateLatestSnapshot(); err != nil {
					require.ErrorIs(rt, err, recordinglog.ErrNoMatchingTermForSnapshot)
				}
			default:
				termRecordingID = drawAppend(
					rt, strconv.Itoa(i), log, usedTerms, termRecordingID, stableRestoreAppendDraw,
				)
			}

			if termRecordingID == -1 {
				require.Equal(rt, recordinglog.NullRecordingID, log.TermRecordingID())
			} else {
				require.Equal(rt, termRecordingID, log.TermRecordingID())
			}
			requireReloadMatchesLogRapid(rt, path, log)
		}
	})
}

func collectValidTerms(entries []recordinglog.Entry) map[int64]recordinglog.Entry {
	terms := make(map[int64]recordinglog.Entry)
	for _, e := range entries {
		if e.Type != recordinglog.EntryTypeTerm || !e.Valid {
			continue
		}
		terms[e.LeadershipTermID] = e
	}
	return terms
}

func TestEnsureCoherentEmptyLog_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		log, path := newLog(t)
		defer func() { require.NoError(rt, log.Close()) }()
		initialTerm := rapid.Int64Range(-2, 10).Draw(rt, "initialTerm")
		lowest := max(int64(0), initialTerm)
		targetTerm := lowest + rapid.Int64Range(1, 6).Draw(rt, "gap")
		initialBase := rapid.Int64Range(0, 1000).Draw(rt, "initialBase")
		termBase := rapid.Int64Range(initialBase, initialBase+1000).Draw(rt, "termBase")
		timestamp := rapid.Int64Range(0, 999999).Draw(rt, "timestamp")

		commit := rapid.Bool().Draw(rt, "commit")
		logPosition := recordinglog.NullPosition
		if commit {
			logPosition = rapid.Int64Range(termBase, termBase+1000).Draw(rt, "logPosition")
		}

		require.NoError(rt, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
			RecordingID:                ensureCoherentRecordingID,
			InitialLogLeadershipTermID: initialTerm,
			InitialTermBaseLogPosition: initialBase,
			LeadershipTermID:           targetTerm,
			TermBaseLogPosition:        termBase,
			LogPosition:                logPosition,
			Timestamp:                  timestamp,
			FileSyncLevel:              recordinglog.FileSyncData,
		}))

		terms := collectValidTerms(log.Entries())
		for termID, entry := range terms {
			require.GreaterOrEqual(rt, entry.LeadershipTermID, lowest)
			require.LessOrEqual(rt, termID, targetTerm)
		}

		require.Len(rt, terms, int(targetTerm-lowest+1))
		for termID := lowest; termID <= targetTerm; termID++ {
			_, ok := terms[termID]
			require.True(rt, ok, "missing term %d", termID)
		}

		// The final loop append commits the prior term (targetTerm-1) to
		// termBase, so only the terms before it carry initialBase.
		for termID := lowest; termID < targetTerm; termID++ {
			entry := terms[termID]
			require.Equal(rt, initialBase, entry.TermBaseLogPosition,
				"term %d base", termID)
			wantLogPosition := initialBase
			if termID == targetTerm-1 {
				wantLogPosition = termBase
			}
			require.Equal(rt, wantLogPosition, entry.LogPosition,
				"term %d logPosition", termID)
		}

		target := terms[targetTerm]
		require.Equal(rt, termBase, target.TermBaseLogPosition, "target base")
		if commit {
			require.Equal(rt, logPosition, target.LogPosition)
		} else {
			require.Equal(rt, recordinglog.NullPosition, target.LogPosition)
		}

		requireReloadMatchesLogRapid(rt, path, log)
	})
}

// termState is the per-term oracle value the EnsureCoherent property test asserts against.
type termState struct {
	termBase    int64
	logPosition int64
}

func TestEnsureCoherentNonEmptyLog_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		log, path := newLog(t)
		defer func() { require.NoError(rt, log.Close()) }()
		const timestamp = 10000
		const step = 100

		initialTerm := rapid.Int64Range(-2, 8).Draw(rt, "initialTerm")
		lowest := max(int64(0), initialTerm)
		seedCount := rapid.Int64Range(1, 5).Draw(rt, "seedCount")
		lastSeed := lowest + seedCount - 1
		seedBase := rapid.Int64Range(0, 1000).Draw(rt, "seedBase")
		base := func(termID int64) int64 { return seedBase + (termID-lowest)*step }
		lastBase := base(lastSeed)

		gap := rapid.Int64Range(0, 6).Draw(rt, "gap")
		target := lastSeed + gap

		// gap == 0 takes the already-at-target branch; leaving the last seeded
		// term uncommitted there keeps "last committed iff logPosition set" exact.
		seedCommitted := gap > 0 && rapid.Bool().Draw(rt, "seedCommitted")
		seedCommit := rapid.Int64Range(lastBase, lastBase+1000).Draw(rt, "seedCommit")
		termBase := rapid.Int64Range(lastBase, lastBase+1000).Draw(rt, "termBase")

		// Seeding terms lowest..lastSeed contiguously: each AppendTerm commits the
		// prior term to the new term's base, so all but the last carry their
		// successor's base as their committed position.
		expected := make(map[int64]termState)
		for termID := lowest; termID <= lastSeed; termID++ {
			expected[termID] = termState{base(termID), recordinglog.NullPosition}
			if termID > lowest {
				prior := expected[termID-1]
				prior.logPosition = base(termID)
				expected[termID-1] = prior
			}
		}

		for termID := lowest; termID <= lastSeed; termID++ {
			require.NoError(rt, log.AppendTerm(ensureCoherentRecordingID, termID, base(termID), timestamp))
		}
		if seedCommitted {
			require.NoError(rt, log.CommitLogPosition(lastSeed, seedCommit))
			last := expected[lastSeed]
			last.logPosition = seedCommit
			expected[lastSeed] = last
		}

		// Model the branch EnsureCoherent takes. Backfill bases take the last
		// term's committed position; an uncommitted last term is first committed
		// to termBase (Case C), a committed one keeps its position (Case D).
		var backfillBase int64
		switch {
		case gap > 0 && seedCommitted:
			backfillBase = seedCommit
		case gap > 0:
			backfillBase = termBase
			last := expected[lastSeed]
			last.logPosition = termBase
			expected[lastSeed] = last
		}

		targetBase := lastBase
		if gap > 0 {
			targetBase = backfillBase
		}
		commit := rapid.Bool().Draw(rt, "commit")
		logPosition := recordinglog.NullPosition
		if commit {
			logPosition = rapid.Int64Range(targetBase, targetBase+1000).Draw(rt, "logPosition")
		}

		if gap > 0 {
			for termID := lastSeed + 1; termID < target; termID++ {
				expected[termID] = termState{backfillBase, backfillBase}
			}
			expected[target] = termState{backfillBase, recordinglog.NullPosition}
		}
		if commit {
			tgt := expected[target]
			tgt.logPosition = logPosition
			expected[target] = tgt
		}

		require.NoError(rt, log.EnsureCoherent(recordinglog.EnsureCoherentParams{
			RecordingID:                ensureCoherentRecordingID,
			InitialLogLeadershipTermID: initialTerm,
			InitialTermBaseLogPosition: seedBase,
			LeadershipTermID:           target,
			TermBaseLogPosition:        termBase,
			LogPosition:                logPosition,
			Timestamp:                  timestamp,
			FileSyncLevel:              recordinglog.FileSyncData,
		}))

		terms := collectValidTerms(log.Entries())
		for termID, entry := range terms {
			require.GreaterOrEqual(rt, entry.LeadershipTermID, lowest)
			require.LessOrEqual(rt, termID, target)
		}

		require.Len(rt, terms, int(target-lowest+1))
		for termID := lowest; termID <= target; termID++ {
			entry, ok := terms[termID]
			require.True(rt, ok, "missing term %d", termID)
			want := expected[termID]
			require.Equal(rt, want.termBase, entry.TermBaseLogPosition, "term %d base", termID)
			require.Equal(rt, want.logPosition, entry.LogPosition, "term %d logPosition", termID)
		}

		for termID := lowest; termID < target; termID++ {
			require.NotEqual(rt, recordinglog.NullPosition, terms[termID].LogPosition,
				"non-target term %d must be committed", termID)
		}
		if commit {
			require.NotEqual(rt, recordinglog.NullPosition, terms[target].LogPosition,
				"target must be committed when logPosition set")
		} else {
			require.Equal(rt, recordinglog.NullPosition, terms[target].LogPosition,
				"target must stay uncommitted when logPosition unset")
		}

		requireReloadMatchesLogRapid(rt, path, log)
	})
}

// minTruncToKeep returns the smallest truncated file length at which the decoder
// still yields rec, mirroring the decoder tolerance: a fixed record needs more
// than its 48-byte header present; a standby record needs the header plus the
// full length-prefixed endpoint, padding excluded.
func minTruncToKeep(rec recordinglog.Entry) int {
	base := int(rec.Position) + recordinglog.EndpointOffset
	if rec.Type == recordinglog.EntryTypeStandbySnapshot {
		return base + atomicbuffer.StringASCIIEncodedLength(len(rec.ArchiveEndpoint))
	}
	return base + 1
}

func TestPartialWriteTolerance_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		log, path := newLog(t)
		usedTerms := make(map[int64]bool)
		termRecordingID := int64(-1)

		// One guaranteed snapshot append so the file holds at least one complete
		// record; the partial-write contract is vacuous on an empty file.
		require.NoError(rt, log.AppendSnapshot(7, 0, 0, 0, 100, 0))

		// Append-only profile suffices for torn-tail coverage: commit and
		// invalidate are single-field in-place writes that change neither byte
		// layout nor record count, so they add nothing a truncation can expose.
		extra := rapid.IntRange(0, 7).Draw(rt, "extra")
		for i := range extra {
			termRecordingID = drawAppend(
				rt, strconv.Itoa(i), log, usedTerms, termRecordingID, randomAppendDraw,
			)
		}
		require.NoError(rt, log.Close())

		fullBytes, err := os.ReadFile(path)
		require.NoError(rt, err)

		fullEntries, err := recordinglog.Reload(path)
		require.NoError(rt, err)
		require.NotEmpty(rt, fullEntries)

		// Recover file order from the index ordinal; appends never reorder bytes
		// and the log holds no removed slots, so indices are contiguous.
		fileOrder := make([]recordinglog.Entry, len(fullEntries))
		for _, e := range fullEntries {
			fileOrder[e.Index] = e
		}

		// Lower-bound the draw so every truncation keeps at least record 0.
		keep0 := minTruncToKeep(fileOrder[0])
		truncAt := rapid.IntRange(keep0, len(fullBytes)).Draw(rt, "truncAt")

		surviveCount := 0
		for _, rec := range fileOrder {
			if truncAt < minTruncToKeep(rec) {
				break
			}
			surviveCount++
		}
		require.GreaterOrEqual(rt, surviveCount, 1)

		require.NoError(rt, os.WriteFile(path, fullBytes[:truncAt], 0o600))

		reloaded, err := recordinglog.Reload(path)
		require.NoError(rt, err)

		expected := slices.Clone(fileOrder[:surviveCount])
		slices.SortStableFunc(expected, recordinglog.CompareEntries)

		require.Len(rt, reloaded, surviveCount)
		require.Equal(rt, expected, reloaded)
	})
}

func TestOpenReloadRoundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		slots := writeLogFile(rt, dir)

		log, err := recordinglog.Open(dir, false)
		require.NoError(rt, err)
		defer func() { require.NoError(rt, log.Close()) }()

		reloaded, err := recordinglog.Reload(filepath.Join(dir, "recording.log"))
		require.NoError(rt, err)
		require.Equal(rt, reloaded, log.Entries())
		require.Equal(rt, slots, log.NextEntryIndex())
	})
}

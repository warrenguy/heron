//go:build rapid

package recordinglog_test

import (
	"flag"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestMain(m *testing.M) {
	flag.Parse()
	userSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "rapid.checks" {
			userSet = true
		}
	})
	if !userSet {
		_ = flag.Set("rapid.checks", "1000")
	}
	os.Exit(m.Run())
}

// drawEntry draws a random Entry. termRecordingID is the shared RecordingID for
// TERM entries (-1 if no TERM drawn yet in this sequence).
func drawEntry(rt *rapid.T, name string, termRecordingID int64) (recordinglog.Entry, int64) {
	rt.Helper()
	entryType := rapid.SampledFrom([]int32{
		recordinglog.EntryTypeTerm,
		recordinglog.EntryTypeSnapshot,
		recordinglog.EntryTypeStandbySnapshot,
	}).Draw(rt, name+"_type")

	recordingID := rapid.Int64Range(0, 1000).Draw(rt, name+"_recordingID")
	if entryType == recordinglog.EntryTypeTerm {
		if termRecordingID == -1 {
			termRecordingID = recordingID
		} else {
			recordingID = termRecordingID
		}
	}

	endpoint := ""
	if entryType == recordinglog.EntryTypeStandbySnapshot {
		epBytes := rapid.SliceOfN(rapid.ByteRange('a', 'z'), 1, 20).Draw(rt, name+"_ep")
		endpoint = string(epBytes)
	}

	e := recordinglog.Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    rapid.Int64Range(0, 10).Draw(rt, name+"_leadershipTermID"),
		TermBaseLogPosition: rapid.Int64Range(0, 1000).Draw(rt, name+"_termBaseLogPos"),
		LogPosition:         rapid.Int64Range(0, 1000).Draw(rt, name+"_logPosition"),
		Timestamp:           rapid.Int64Range(0, 999999).Draw(rt, name+"_timestamp"),
		ServiceID:           rapid.Int32Range(0, 10).Draw(rt, name+"_serviceID"),
		Type:                entryType,
		Valid:               rapid.Bool().Draw(rt, name+"_valid"),
		ArchiveEndpoint:     endpoint,
	}
	return e, termRecordingID
}

func TestRoundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 8).Draw(rt, "n")
		termRecordingID := int64(-1)
		var expected []recordinglog.Entry
		var buf []byte
		pos := int64(0)
		for i := range n {
			e, updated := drawEntry(rt, strconv.Itoa(i), termRecordingID)
			termRecordingID = updated
			e.Position = pos
			e.Index = int32(i)
			encoded := recordinglog.EncodeEntry(e)
			buf = append(buf, encoded...)
			pos += int64(len(encoded))
			expected = append(expected, e)
		}

		got, err := recordinglog.DecodeEntries(buf)
		require.NoError(rt, err)
		require.Len(rt, got, len(expected))
		for i, want := range expected {
			require.Equal(rt, want, got[i])
		}
	})
}

func TestNoPanic_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 4096).Draw(rt, "n")
		buf := rapid.SliceOfN(rapid.Byte(), n, n).Draw(rt, "buf")
		require.NotPanics(rt, func() {
			_, _ = recordinglog.DecodeEntries(buf)
		})
	})
}

func TestTruncationInvariant_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 6).Draw(rt, "n")
		termRecordingID := int64(-1)
		var buf []byte
		for i := range n {
			e, updated := drawEntry(rt, strconv.Itoa(i), termRecordingID)
			termRecordingID = updated
			buf = append(buf, recordinglog.EncodeEntry(e)...)
		}

		fullEntries, err := recordinglog.DecodeEntries(buf)
		require.NoError(rt, err)

		truncAt := rapid.IntRange(0, len(buf)).Draw(rt, "truncAt")
		truncated, truncErr := recordinglog.DecodeEntries(buf[:truncAt])

		// Truncation of valid input never produces an error.
		require.NoError(rt, truncErr)
		require.LessOrEqual(rt, len(truncated), len(fullEntries))
		for i, got := range truncated {
			require.Equal(rt, fullEntries[i], got)
		}
	})
}

// lessModel is an independent reference oracle for TestComparatorModel_rapid.
// It branches on a.Type/b.Type individually rather than computing snapshot booleans,
// so it differs structurally from the Go less and can catch their divergence.
func lessModel(a, b recordinglog.Entry) bool {
	if a.LeadershipTermID != b.LeadershipTermID {
		return a.LeadershipTermID < b.LeadershipTermID
	}
	if a.TermBaseLogPosition != b.TermBaseLogPosition {
		return a.TermBaseLogPosition < b.TermBaseLogPosition
	}
	if a.Type != b.Type {
		if a.Type == recordinglog.EntryTypeSnapshot {
			// snapshot sorts after every other type; a does not come before b
			return false
		}
		if b.Type == recordinglog.EntryTypeSnapshot {
			// b is snapshot; a sorts before b
			return true
		}
		// natural int32 order for all non-snapshot pairs
		return a.Type < b.Type
	}
	if a.Type == recordinglog.EntryTypeTerm {
		return a.Index < b.Index
	}
	if a.LogPosition != b.LogPosition {
		return a.LogPosition < b.LogPosition
	}
	return b.ServiceID < a.ServiceID
}

func drawComparatorEntry(rt *rapid.T, prefix string) recordinglog.Entry {
	rt.Helper()
	return recordinglog.Entry{
		LeadershipTermID:    rapid.Int64Range(0, 5).Draw(rt, prefix+"_leadershipTermID"),
		TermBaseLogPosition: rapid.Int64Range(0, 5).Draw(rt, prefix+"_termBaseLogPos"),
		LogPosition:         rapid.Int64Range(0, 5).Draw(rt, prefix+"_logPosition"),
		ServiceID:           rapid.Int32Range(0, 5).Draw(rt, prefix+"_serviceID"),
		Type:                rapid.Int32Range(0, 3).Draw(rt, prefix+"_type"),
		Index:               rapid.Int32Range(0, 5).Draw(rt, prefix+"_index"),
	}
}

func TestComparatorModel_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a := drawComparatorEntry(rt, "a")
		b := drawComparatorEntry(rt, "b")
		require.Equal(rt, lessModel(a, b), recordinglog.Less(a, b))
	})
}

func TestComparatorStrictWeakOrdering_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		entries := make([]recordinglog.Entry, rapid.IntRange(0, 8).Draw(rt, "n"))
		for i := range entries {
			entry := drawComparatorEntry(rt, strconv.Itoa(i))
			entry.Index = int32(i)
			entries[i] = entry
		}
		less := recordinglog.Less
		equiv := func(x, y recordinglog.Entry) bool { return !less(x, y) && !less(y, x) }
		for i := range entries {
			require.Falsef(rt, less(entries[i], entries[i]), "irreflexivity at %d", i)
			for j := range entries {
				if less(entries[i], entries[j]) {
					require.Falsef(rt, less(entries[j], entries[i]), "asymmetry at %d,%d", i, j)
				}
				for k := range entries {
					if less(entries[i], entries[j]) && less(entries[j], entries[k]) {
						require.Truef(
							rt,
							less(entries[i], entries[k]),
							"less transitivity at %d,%d,%d",
							i, j, k,
						)
					}
					if equiv(entries[i], entries[j]) && equiv(entries[j], entries[k]) {
						require.Truef(
							rt,
							equiv(entries[i], entries[k]),
							"equivalence transitivity at %d,%d,%d",
							i, j, k,
						)
					}
				}
			}
		}
		slices.SortStableFunc(entries, recordinglog.CompareEntries)
		for i := 1; i < len(entries); i++ {
			require.Falsef(rt, less(entries[i], entries[i-1]), "output not ordered at %d", i)
		}
	})
}

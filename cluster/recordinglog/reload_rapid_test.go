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

func TestRoundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 8).Draw(rt, "n")
		termRecordingID := int64(-1)
		var expected []recordinglog.Entry
		var buf []byte
		pos := int64(0)
		for i := range n {
			e, updated := recordinglog.DrawEntry(rt, strconv.Itoa(i), termRecordingID)
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
			e, updated := recordinglog.DrawEntry(rt, strconv.Itoa(i), termRecordingID)
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

// lessModel restates the upstream RecordingLog comparator for
// TestComparatorModel_rapid. It follows less branch for branch. The property
// test catches an edit to one of the two, not a logic error both share.
func lessModel(a, b recordinglog.Entry) bool {
	if a.LeadershipTermID != b.LeadershipTermID {
		return a.LeadershipTermID < b.LeadershipTermID
	}
	if a.TermBaseLogPosition != b.TermBaseLogPosition {
		return a.TermBaseLogPosition < b.TermBaseLogPosition
	}
	if a.Type != b.Type {
		if a.Type == recordinglog.EntryTypeTerm {
			// term sorts before every other type
			return true
		}
		if b.Type == recordinglog.EntryTypeTerm {
			// b is term; a does not come before b
			return false
		}
	} else if a.Type == recordinglog.EntryTypeTerm {
		return a.Index < b.Index
	}
	if a.LogPosition != b.LogPosition {
		return a.LogPosition < b.LogPosition
	}
	if a.Type != b.Type {
		// reverse type order at one log position
		return b.Type < a.Type
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

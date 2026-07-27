package recordinglog_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/recordinglog"
)

func entry(
	leadershipTermID int64,
	termBaseLogPosition int64,
	logPosition int64,
	serviceID int32,
	entryType int32,
	index int32,
) recordinglog.Entry {
	return recordinglog.Entry{
		LeadershipTermID:    leadershipTermID,
		TermBaseLogPosition: termBaseLogPosition,
		LogPosition:         logPosition,
		ServiceID:           serviceID,
		Type:                entryType,
		Index:               index,
		Valid:               true,
		RecordingID:         1,
	}
}

// TestLess asserts entry comparator branch order used by Reload.
func TestLess(t *testing.T) {
	t.Parallel()

	type lessCase struct {
		name   string
		a, b   recordinglog.Entry
		wantAB bool // less(a, b)
		wantBA bool // less(b, a)
	}

	cases := []lessCase{
		{
			name:   "leadershipTermId ascending: a < b",
			a:      entry(1, 0, 0, 0, recordinglog.EntryTypeTerm, 0),
			b:      entry(2, 0, 0, 0, recordinglog.EntryTypeTerm, 1),
			wantAB: true,
			wantBA: false,
		},
		{
			name:   "leadershipTermId equal, termBaseLogPosition ascending: a < b",
			a:      entry(1, 10, 0, 0, recordinglog.EntryTypeTerm, 0),
			b:      entry(1, 20, 0, 0, recordinglog.EntryTypeTerm, 1),
			wantAB: true,
			wantBA: false,
		},
		{
			name:   "snapshot sorts after non-snapshot: TERM before SNAPSHOT",
			a:      entry(1, 0, 0, 0, recordinglog.EntryTypeTerm, 0),
			b:      entry(1, 0, 0, 0, recordinglog.EntryTypeSnapshot, 0),
			wantAB: true,
			wantBA: false,
		},
		{
			name:   "TERM sorts before STANDBY_SNAPSHOT: natural type order",
			a:      entry(1, 0, 0, 0, recordinglog.EntryTypeTerm, 0),
			b:      entry(1, 0, 0, 0, recordinglog.EntryTypeStandbySnapshot, 0),
			wantAB: true,
			wantBA: false,
		},
		{
			name:   "STANDBY_SNAPSHOT sorts before SNAPSHOT: snapshot-exception branch fires",
			a:      entry(1, 0, 0, 0, recordinglog.EntryTypeSnapshot, 0),
			b:      entry(1, 0, 0, 0, recordinglog.EntryTypeStandbySnapshot, 0),
			wantAB: false,
			wantBA: true,
		},
		{
			name:   "both TERM: Index tiebreak ascending",
			a:      entry(1, 0, 0, 0, recordinglog.EntryTypeTerm, 0),
			b:      entry(1, 0, 0, 0, recordinglog.EntryTypeTerm, 1),
			wantAB: true,
			wantBA: false,
		},
		{
			name:   "both TERM same Index: neither less",
			a:      entry(1, 0, 0, 0, recordinglog.EntryTypeTerm, 5),
			b:      entry(1, 0, 0, 0, recordinglog.EntryTypeTerm, 5),
			wantAB: false,
			wantBA: false,
		},
		{
			name:   "both SNAPSHOT: logPosition ascending",
			a:      entry(1, 0, 100, 0, recordinglog.EntryTypeSnapshot, 0),
			b:      entry(1, 0, 200, 0, recordinglog.EntryTypeSnapshot, 1),
			wantAB: true,
			wantBA: false,
		},
		{
			name:   "both SNAPSHOT same logPosition: serviceId descending",
			a:      entry(1, 0, 100, 5, recordinglog.EntryTypeSnapshot, 0),
			b:      entry(1, 0, 100, 3, recordinglog.EntryTypeSnapshot, 1),
			wantAB: true, // a.ServiceID(5) > b.ServiceID(3) -> a sorts before b
			wantBA: false,
		},
		{
			name:   "both SNAPSHOT same logPosition and serviceId: neither less",
			a:      entry(1, 0, 100, 2, recordinglog.EntryTypeSnapshot, 0),
			b:      entry(1, 0, 100, 2, recordinglog.EntryTypeSnapshot, 1),
			wantAB: false,
			wantBA: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.wantAB, recordinglog.Less(tc.a, tc.b))
			require.Equal(t, tc.wantBA, recordinglog.Less(tc.b, tc.a))
		})
	}
}

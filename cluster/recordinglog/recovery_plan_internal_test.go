package recordinglog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryPredicates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		entry Entry
		want  bool
		fn    func(Entry) bool
	}{
		{
			name:  "valid standby snapshot excluded",
			entry: Entry{Valid: true, Type: EntryTypeStandbySnapshot},
			want:  false,
			fn:    isValidSnapshot,
		},
		{
			name:  "valid snapshot included",
			entry: Entry{Valid: true, Type: EntryTypeSnapshot},
			want:  true,
			fn:    isValidSnapshot,
		},
		{
			name:  "invalid snapshot excluded",
			entry: Entry{Valid: false, Type: EntryTypeSnapshot},
			want:  false,
			fn:    isValidSnapshot,
		},
		{
			name:  "valid term excluded from snapshot predicate",
			entry: Entry{Valid: true, Type: EntryTypeTerm},
			want:  false,
			fn:    isValidSnapshot,
		},
		{
			name:  "valid term included",
			entry: Entry{Valid: true, Type: EntryTypeTerm},
			want:  true,
			fn:    isValidTerm,
		},
		{
			name:  "snapshot rejected from term predicate",
			entry: Entry{Valid: true, Type: EntryTypeSnapshot},
			want:  false,
			fn:    isValidTerm,
		},
		{
			name:  "invalid term excluded",
			entry: Entry{Valid: false, Type: EntryTypeTerm},
			want:  false,
			fn:    isValidTerm,
		},
		{
			name: "valid cm snapshot included",
			entry: Entry{
				Valid:     true,
				Type:      EntryTypeSnapshot,
				ServiceID: consensusModuleServiceID,
			},
			want: true,
			fn:   isValidConsensusModuleSnapshot,
		},
		{
			name: "standby cm snapshot excluded",
			entry: Entry{
				Valid:     true,
				Type:      EntryTypeStandbySnapshot,
				ServiceID: consensusModuleServiceID,
			},
			want: false,
			fn:   isValidConsensusModuleSnapshot,
		},
		{
			name: "service snapshot excluded from cm predicate",
			entry: Entry{
				Valid:     true,
				Type:      EntryTypeSnapshot,
				ServiceID: 0,
			},
			want: false,
			fn:   isValidConsensusModuleSnapshot,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.fn(tc.entry))
		})
	}
}

func snapshotEntry(
	recordingID, leadershipTermID, termBaseLogPosition, logPosition int64,
	serviceID int32,
) Entry {
	return Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    leadershipTermID,
		TermBaseLogPosition: termBaseLogPosition,
		LogPosition:         logPosition,
		ServiceID:           serviceID,
		Type:                EntryTypeSnapshot,
		Valid:               true,
	}
}

func termEntryForAddSnapshots(recordingID int64) Entry {
	return Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    1,
		TermBaseLogPosition: 1440,
		LogPosition:         2880,
		Type:                EntryTypeTerm,
		Valid:               true,
	}
}

func TestSelectRecoveryAnchor_ordersSnapshotsByServiceID(t *testing.T) {
	t.Parallel()

	term := termEntryForAddSnapshots(0)
	entries := []Entry{
		term,
		snapshotEntry(4, 1, 1440, 2880, 2),
		snapshotEntry(5, 1, 1440, 2880, 1),
		snapshotEntry(6, 1, 1440, 2880, 0),
		snapshotEntry(7, 1, 1440, 2880, consensusModuleServiceID),
	}

	sel, err := selectRecoveryAnchor(entries, 3)
	require.NoError(t, err)
	require.True(t, sel.hasSnapshot)
	require.Len(t, sel.snapshots, 4)
	require.Equal(t, consensusModuleServiceID, sel.snapshots[0].ServiceID)
	require.Equal(t, int32(0), sel.snapshots[1].ServiceID)
	require.Equal(t, int32(1), sel.snapshots[2].ServiceID)
	require.Equal(t, int32(2), sel.snapshots[3].ServiceID)
}

func TestSelectRecoveryAnchor_allowsPartialSnapshotGroup(t *testing.T) {
	t.Parallel()

	entries := []Entry{
		termEntryForAddSnapshots(0),
		snapshotEntry(5, 1, 1440, 2880, 1),
		snapshotEntry(6, 1, 1440, 2880, 0),
		snapshotEntry(7, 1, 1440, 2880, consensusModuleServiceID),
	}

	sel, err := selectRecoveryAnchor(entries, 3)
	require.NoError(t, err)
	require.True(t, sel.hasSnapshot)
	require.Len(t, sel.snapshots, 3)
	require.Equal(t, consensusModuleServiceID, sel.snapshots[0].ServiceID)
	require.Equal(t, int32(0), sel.snapshots[1].ServiceID)
	require.Equal(t, int32(1), sel.snapshots[2].ServiceID)
}

func TestMatchesConsensusModuleSnapshotGroup(t *testing.T) {
	t.Parallel()

	cm := snapshotFromEntry(snapshotEntry(7, 1, 1440, 2880, consensusModuleServiceID))
	service := snapshotEntry(6, 1, 1440, 2880, 0)

	require.True(t, matchesConsensusModuleSnapshotGroup(service, cm))
	require.False(t, matchesConsensusModuleSnapshotGroup(
		snapshotEntry(6, 1, 1440, 2881, 0),
		cm,
	))
	invalidService := snapshotEntry(6, 1, 1440, 2880, 0)
	invalidService.Valid = false
	require.True(t, matchesConsensusModuleSnapshotGroup(invalidService, cm))
}

func TestSelectRecoveryAnchor_rejectsOutOfRangeServiceID(t *testing.T) {
	t.Parallel()

	entries := []Entry{
		termEntryForAddSnapshots(0),
		snapshotEntry(4, 1, 1440, 2880, 2),
		snapshotEntry(5, 1, 1440, 2880, 0),
		snapshotEntry(7, 1, 1440, 2880, consensusModuleServiceID),
	}

	_, err := selectRecoveryAnchor(entries, 2)
	require.Error(t, err)
	var outOfRange *SnapshotServiceIDOutOfRangeError
	require.ErrorAs(t, err, &outOfRange)
	require.ErrorIs(t, err, ErrSnapshotServiceIDOutOfRange)
	require.Equal(t, int32(2), outOfRange.ServiceID)
	require.Equal(t, 2, outOfRange.ServiceCount)
}

func TestSelectRecoveryAnchor_missingServiceSnapshot(t *testing.T) {
	t.Parallel()

	entries := []Entry{
		snapshotEntry(7, 1, 1440, 2880, consensusModuleServiceID),
	}

	_, err := selectRecoveryAnchor(entries, 1)
	require.Error(t, err)
	var missing *MissingServiceSnapshotError
	require.ErrorAs(t, err, &missing)
	require.ErrorIs(t, err, ErrSnapshotMissingForService)
	require.Equal(t, 1, missing.ServiceIndex)
}

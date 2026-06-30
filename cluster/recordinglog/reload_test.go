package recordinglog_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/recordinglog"
)

// Wire offsets mirrored locally only for malformed-buffer edge cases.
const oversizedEndpointLen = recordinglog.MaxEndpointLen + 1

func termEntry(recordingID int64) recordinglog.Entry {
	return recordinglog.Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    10,
		TermBaseLogPosition: 100,
		LogPosition:         200,
		Timestamp:           1000,
		ServiceID:           0,
		Type:                recordinglog.EntryTypeTerm,
		Valid:               true,
	}
}

func nullSlotBuf(n int) []byte {
	buf := make([]byte, n)
	binary.LittleEndian.PutUint32(buf[recordinglog.EntryTypeOffset:], 0xFFFFFFFF)
	return buf
}

func standbyBuf(n int) []byte {
	buf := make([]byte, n)
	standbyType := uint32(recordinglog.EntryTypeStandbySnapshot)
	binary.LittleEndian.PutUint32(buf[recordinglog.EntryTypeOffset:], standbyType)
	return buf
}

func standbyEndpointBuf(n int, endpointLen int32) []byte {
	buf := standbyBuf(n)
	binary.LittleEndian.PutUint32(buf[recordinglog.EndpointOffset:], uint32(endpointLen))
	return buf
}

func twoTerms(id1, id2 int64) []byte {
	buf := recordinglog.EncodeEntry(termEntry(id1))
	return append(buf, recordinglog.EncodeEntry(termEntry(id2))...)
}

func TestDecodeEntries_singleEntryRoundtrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		entry recordinglog.Entry
	}{
		{
			name:  "TERM",
			entry: termEntry(42),
		},
		{
			name: "STANDBY_SNAPSHOT with endpoint",
			entry: recordinglog.Entry{
				RecordingID:         7,
				LeadershipTermID:    2,
				TermBaseLogPosition: 0,
				LogPosition:         512,
				Timestamp:           9999,
				ServiceID:           1,
				Type:                recordinglog.EntryTypeStandbySnapshot,
				Valid:               true,
				ArchiveEndpoint:     "localhost:9091",
			},
		},
		{
			name: "invalidated SNAPSHOT: flag stripped, Valid false",
			entry: recordinglog.Entry{
				RecordingID: 5,
				Type:        recordinglog.EntryTypeSnapshot,
				Valid:       false,
			},
		},
		{
			name: "unknown type word accepted as opaque",
			entry: recordinglog.Entry{
				RecordingID: 99,
				Type:        7,
				Valid:       true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			entries, err := recordinglog.DecodeEntries(recordinglog.EncodeEntry(tc.entry))
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, tc.entry, entries[0])
		})
	}
}

func TestDecodeEntries_emptyResult(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		buf  []byte
	}{
		{"nil buffer", nil},
		{"47 bytes: below the fixed header", make([]byte, 47)},
		{"48 bytes: strict-< guard never enters the loop", make([]byte, 48)},
		{"63 bytes null slot: enters loop, skips slot, advances 64, exits", nullSlotBuf(63)},
		{"standby length prefix past EOF: epOff+4 exceeds len", standbyBuf(51)},
		{"standby payload past EOF: epLen exceeds remaining", standbyEndpointBuf(56, 5)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			entries, err := recordinglog.DecodeEntries(tc.buf)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestDecodeEntries_errors(t *testing.T) {
	t.Parallel()
	nullRecordingTerm := recordinglog.EncodeEntry(recordinglog.Entry{
		RecordingID: -1,
		Type:        recordinglog.EntryTypeTerm,
		Valid:       true,
	})
	tests := []struct {
		name    string
		buf     []byte
		wantErr error
	}{
		{
			name: "endpoint length above maximum",
			buf: standbyEndpointBuf(
				recordinglog.EndpointOffset+4+oversizedEndpointLen+recordinglog.RecordAlignment,
				int32(oversizedEndpointLen),
			),
			wantErr: recordinglog.ErrEndpointTooLong,
		},
		{
			name:    "endpoint length negative",
			buf:     standbyEndpointBuf(128, -1),
			wantErr: recordinglog.ErrEndpointTooLong,
		},
		{
			name:    "standby endpoint empty",
			buf:     standbyEndpointBuf(128, 0),
			wantErr: recordinglog.ErrStandbyEndpointEmpty,
		},
		{
			name:    "two TERMs with different recordingId",
			buf:     twoTerms(1, 2),
			wantErr: recordinglog.ErrTermRecordingIDMismatch,
		},
		{
			name:    "TERM with NULL_VALUE recordingId",
			buf:     nullRecordingTerm,
			wantErr: recordinglog.ErrTermRecordingIDInvalid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := recordinglog.DecodeEntries(tc.buf)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestDecodeEntries_nullSlotSkipped(t *testing.T) {
	t.Parallel()
	// A removed first slot must not shift the surviving entry's file-order
	// Position and Index.
	buf := nullSlotBuf(128)
	copy(buf[recordinglog.RecordAlignment:], recordinglog.EncodeEntry(termEntry(3)))

	entries, err := recordinglog.DecodeEntries(buf)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, int64(recordinglog.RecordAlignment), entries[0].Position)
	require.Equal(t, int32(1), entries[0].Index)
	require.Equal(t, int64(3), entries[0].RecordingID)
}

func TestDecodeEntries_truncatedFinalEntry(t *testing.T) {
	t.Parallel()
	// Fewer than 49 bytes after a full record cannot re-enter the decode loop.
	buf := make([]byte, recordinglog.RecordAlignment+30)
	copy(buf, recordinglog.EncodeEntry(termEntry(1)))

	entries, err := recordinglog.DecodeEntries(buf)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, int64(0), entries[0].Position)
}

func TestDecodeEntries_zeroTailAfterTerm(t *testing.T) {
	t.Parallel()
	// 49-byte zero tail re-enters the loop as a spurious TERM with mismatched recordingId.
	buf := make([]byte, recordinglog.RecordAlignment+49)
	copy(buf, recordinglog.EncodeEntry(termEntry(1)))

	_, err := recordinglog.DecodeEntries(buf)
	require.ErrorIs(t, err, recordinglog.ErrTermRecordingIDMismatch)
}

func TestDecodeEntries_twoTermsSameRecordingID(t *testing.T) {
	t.Parallel()
	entries, err := recordinglog.DecodeEntries(twoTerms(42, 42))
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestDecodeEntries_64bytesOneEntry(t *testing.T) {
	t.Parallel()
	// Documents the true single-entry decode boundary (contrast: 63-byte null-slot test).
	buf := recordinglog.EncodeEntry(termEntry(7))
	require.Len(t, buf, recordinglog.RecordAlignment)
	entries, err := recordinglog.DecodeEntries(buf)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, int64(7), entries[0].RecordingID)
}

func TestReload_basic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "recording.log")

	term := termEntry(5)
	snap := recordinglog.Entry{
		RecordingID:         6,
		LeadershipTermID:    10,
		TermBaseLogPosition: 100,
		LogPosition:         300,
		Timestamp:           2000,
		ServiceID:           0,
		Type:                recordinglog.EntryTypeSnapshot,
		Valid:               true,
	}
	var content []byte
	content = append(content, recordinglog.EncodeEntry(term)...)
	content = append(content, recordinglog.EncodeEntry(snap)...)
	err := os.WriteFile(path, content, 0o600)
	require.NoError(t, err)

	entries, err := recordinglog.Reload(path)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	// Same leadershipTermId and termBaseLogPosition: TERM sorts before SNAPSHOT
	// because a snapshot follows a non-snapshot at the same key.
	require.Equal(t, recordinglog.EntryTypeTerm, entries[0].Type)
	require.Equal(t, recordinglog.EntryTypeSnapshot, entries[1].Type)
}

func TestReload_missingFile(t *testing.T) {
	t.Parallel()
	_, err := recordinglog.Reload("/nonexistent/path/recording.log")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestReload_emptyFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "recording.log")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	entries, err := recordinglog.Reload(path)
	require.NoError(t, err)
	require.Empty(t, entries)
}

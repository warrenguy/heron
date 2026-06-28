package recordinglog

import "math"

// Entry type constants. Values match the recording.log on-disk encoding.
const (
	EntryTypeTerm            int32 = 0
	EntryTypeSnapshot        int32 = 1
	EntryTypeStandbySnapshot int32 = 2

	// entryTypeInvalidFlag is OR'd into the type word to mark an entry invalid.
	// It is the high bit of the int32 word at offset 44.
	entryTypeInvalidFlag int32 = math.MinInt32

	// nullEntryType marks a removed slot; Reload skips it.
	nullEntryType int32 = -1
)

// hasEndpoint reports whether entryType carries a length-prefixed archive
// endpoint after the fixed header. Only STANDBY_SNAPSHOT does; this is the
// single source of truth shared by the decoder, the wire-size calculation, and
// the test encoder.
func hasEndpoint(entryType int32) bool {
	return entryType == EntryTypeStandbySnapshot
}

// Entry is one decoded recording.log record.
//
// A recording.log file is a sequence of fixed-size, 64-byte-aligned records
// that map leadership terms and snapshots to Archive recording IDs. Wire layout
// matches Aeron 1.51.0: little-endian fields, 64-byte record alignment.
//
// Type holds the entry classification with the invalid flag already
// stripped. Valid reports whether the invalid flag was absent. ArchiveEndpoint
// is populated only for EntryTypeStandbySnapshot entries and is empty for all
// others.
//
// Position is the byte offset of this entry in the file. Index is the
// zero-based ordinal in file order before the comparator sort; both are needed
// by the comparator tiebreak and by callers that rewrite fields in place.
type Entry struct {
	RecordingID         int64
	LeadershipTermID    int64
	TermBaseLogPosition int64
	LogPosition         int64
	Timestamp           int64
	ServiceID           int32
	Type                int32
	ArchiveEndpoint     string
	Valid               bool
	Position            int64
	Index               int32
}

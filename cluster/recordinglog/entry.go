package recordinglog

import (
	"math"

	"github.com/warrenguy/heron/atomicbuffer"
)

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

func isSnapshotType(entryType int32) bool {
	return entryType == EntryTypeSnapshot || hasEndpoint(entryType)
}

// isValidSnapshot and isValidTerm live here (not recovery_plan.go) because log.go
// also calls isValidTerm; recovery uses isValidSnapshot only (excludes standby).
func isValidSnapshot(e Entry) bool {
	return e.Valid && e.Type == EntryTypeSnapshot
}

func isValidTerm(e Entry) bool {
	return e.Valid && e.Type == EntryTypeTerm
}

func isValidConsensusModuleSnapshot(e Entry) bool {
	return isValidSnapshot(e) && e.ServiceID == consensusModuleServiceID
}

// Entry is one decoded recording.log record.
//
// A recording.log file is a sequence of 64-byte-aligned records that map
// leadership terms and snapshots to Archive recording IDs. TERM, SNAPSHOT, and
// removed slots occupy exactly 64 bytes; STANDBY_SNAPSHOT entries may span a
// larger aligned region when carrying a length-prefixed archive endpoint.
// Wire layout: little-endian fields, 64-byte record alignment.
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

// encodeEntry encodes e into a zero-padded byte slice using the recording.log
// wire layout. It is the single encode reference for both valid and invalid
// entries: when Valid is false, entryTypeInvalidFlag is OR'd into the type word,
// matching the in-place invalidation flag the decoder strips on read.
//
// The integer writes mirror the decoder's atomic reads; the endpoint is written
// raw after its 4-byte little-endian length to match the decoder's tolerant
// manual read. A fresh slice is allocated per call.
func encodeEntry(e Entry) []byte {
	endpoint := e.ArchiveEndpoint
	buf := make([]byte, alignedEntryLength(e.Type, endpoint))
	buffer := atomicbuffer.Wrap(buf)
	buffer.PutInt64(recordingIDOffset, e.RecordingID)
	buffer.PutInt64(leadershipTermIDOffset, e.LeadershipTermID)
	buffer.PutInt64(termBaseLogPositionOffset, e.TermBaseLogPosition)
	buffer.PutInt64(logPositionOffset, e.LogPosition)
	buffer.PutInt64(timestampOffset, e.Timestamp)
	buffer.PutInt32(serviceIDOffset, e.ServiceID)

	typeWord := e.Type
	if !e.Valid {
		typeWord |= entryTypeInvalidFlag
	}
	buffer.PutInt32(entryTypeOffset, typeWord)

	if hasEndpoint(e.Type) {
		//nolint:gosec // G115: the endpoint is at most maxEndpointLen
		buffer.PutInt32(endpointOffset, int32(len(endpoint)))
		copy(buf[endpointOffset+atomicbuffer.StringASCIIHeaderLength:], endpoint)
	}
	return buf
}

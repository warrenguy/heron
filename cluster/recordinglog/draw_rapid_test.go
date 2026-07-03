//go:build rapid

package recordinglog

import (
	"strconv"

	"pgregory.net/rapid"
)

var rapidEntryTypes = []int32{
	EntryTypeTerm,
	EntryTypeSnapshot,
	EntryTypeStandbySnapshot,
}

// DrawEntry draws a random Entry. termRecordingID is the shared RecordingID for
// TERM entries (-1 if no TERM drawn yet in this sequence).
func DrawEntry(rt *rapid.T, name string, termRecordingID int64) (Entry, int64) {
	rt.Helper()
	entryType := rapid.SampledFrom(rapidEntryTypes).Draw(rt, name+"_type")

	recordingID := rapid.Int64Range(0, 1000).Draw(rt, name+"_recordingID")
	if entryType == EntryTypeTerm {
		if termRecordingID == -1 {
			termRecordingID = recordingID
		} else {
			recordingID = termRecordingID
		}
	}

	endpoint := ""
	if entryType == EntryTypeStandbySnapshot {
		epBytes := rapid.SliceOfN(rapid.ByteRange('a', 'z'), 1, 20).Draw(rt, name+"_ep")
		endpoint = string(epBytes)
	}

	var logPosition int64
	switch entryType {
	case EntryTypeTerm:
		logPosition = rapid.OneOf(
			rapid.Just(NullPosition),
			rapid.Int64Range(0, 1000),
		).Draw(rt, name+"_termLogPosition")
	default:
		logPosition = rapid.Int64Range(0, 1000).Draw(rt, name+"_logPosition")
	}

	e := Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    rapid.Int64Range(0, 10).Draw(rt, name+"_leadershipTermID"),
		TermBaseLogPosition: rapid.Int64Range(0, 1000).Draw(rt, name+"_termBaseLogPos"),
		LogPosition:         logPosition,
		Timestamp:           rapid.Int64Range(0, 999999).Draw(rt, name+"_timestamp"),
		ServiceID:           rapid.Int32Range(-1, 10).Draw(rt, name+"_serviceID"),
		Type:                entryType,
		Valid:               rapid.Bool().Draw(rt, name+"_valid"),
		ArchiveEndpoint:     endpoint,
	}
	return e, termRecordingID
}

// DrawIndexedEntries draws n entries with stable Index and Position fields.
func DrawIndexedEntries(rt *rapid.T, n int, name string) []Entry {
	rt.Helper()
	entries := make([]Entry, n)
	termRecordingID := int64(-1)
	for i := range n {
		e, updated := DrawEntry(rt, name+strconv.Itoa(i), termRecordingID)
		termRecordingID = updated
		e.Index = int32(i)
		e.Position = int64(i) * recordAlignment
		entries[i] = e
	}
	return entries
}

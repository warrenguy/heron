package recordinglog

// DecodeEntries exposes the internal decodeEntries function for black-box tests.
var DecodeEntries = decodeEntries

// Less exposes the internal less comparator for black-box tests.
var Less = less

// CompareEntries exposes the internal compareEntries ordering for black-box tests.
var CompareEntries = compareEntries

// EncodeEntry re-exports the production encodeEntry so tests share the single
// encode reference and cannot drift from the bytes append writes.
var EncodeEntry = encodeEntry

// Black-box test exports for recordinglog_test.

const (
	MaxEndpointLen    = maxEndpointLen
	RecordAlignment   = recordAlignment
	LogPositionOffset = logPositionOffset
	EntryTypeOffset   = entryTypeOffset
	EndpointOffset    = endpointOffset
)

// FindLastTerm exposes the internal findLastTerm helper for black-box tests.
func FindLastTerm(l *Log) *Entry {
	return l.findLastTerm()
}

// InvalidSnapshots exposes the cache slot indices of invalid snapshots for
// black-box tests that assert the append index shift.
func InvalidSnapshots(l *Log) []int32 {
	return l.invalidSnapshots
}

// InvalidateEntry exposes the internal invalidateEntry mutator for black-box
// tests asserting the single-field invalidation write.
func InvalidateEntry(l *Log, index int32) error {
	return l.invalidateEntry(index)
}

// WriteFieldAt exposes the single-field write helper for the zero-alloc gate.
func WriteFieldAt(l *Log, position int64, offset int, b []byte) error {
	return l.writeFieldAt(position, offset, b)
}

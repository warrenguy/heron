package recordinglog

import (
	"errors"
	"os"

	"github.com/warrenguy/heron/atomicbuffer"
)

var (
	// ErrTermRecordingIDInvalid reports a TERM entry with recordingId == NULL_VALUE (-1).
	ErrTermRecordingIDInvalid = errors.New("recordinglog: TERM recordingId is NULL_VALUE")

	// ErrTermRecordingIDMismatch reports TERM entries with inconsistent recordingIds.
	ErrTermRecordingIDMismatch = errors.New("recordinglog: inconsistent TERM recordingId")

	// ErrEndpointTooLong reports a negative or oversized standby endpoint length.
	ErrEndpointTooLong = errors.New("recordinglog: endpoint length negative or exceeds maximum")

	// ErrStandbyEndpointEmpty reports a zero-length standby snapshot endpoint.
	ErrStandbyEndpointEmpty = errors.New("recordinglog: standby snapshot endpoint is empty")

	// ErrDuplicateTerm reports an AppendTerm for a leadershipTermId already in the log.
	ErrDuplicateTerm = errors.New("recordinglog: duplicate TERM entry for leadershipTermId")

	// ErrShortWrite reports a WriteAt that persisted fewer bytes than requested.
	ErrShortWrite = errors.New("recordinglog: short write")

	// ErrInvalidRecordingID reports a snapshot append with recordingId == NULL_VALUE (-1).
	ErrInvalidRecordingID = errors.New("recordinglog: recordingId is NULL_VALUE")

	// ErrNoMatchingTermForSnapshot reports a snapshot whose leadershipTermId has no term entry.
	ErrNoMatchingTermForSnapshot = errors.New("recordinglog: no matching term for snapshot")

	// ErrUnknownLeadershipTermID reports a commit for a leadershipTermId absent from the log.
	ErrUnknownLeadershipTermID = errors.New("recordinglog: unknown leadershipTermId")

	// ErrPriorTermNotCommitted reports an EnsureCoherent whose last term is
	// uncommitted and which supplied no termBaseLogPosition to commit it.
	ErrPriorTermNotCommitted = errors.New("recordinglog: prior term was not committed")
)

const nullValue int64 = -1

// NullPosition marks an uncommitted logPosition. It shares the value -1 with the
// Aeron NULL_VALUE and NULL_POSITION sentinels.
const NullPosition int64 = nullValue

// Reload reads path, decodes entries, sorts them in comparator order, and
// returns the slice.
//
// Truncation at EOF is ignored when fewer than 49 bytes remain at the current
// record offset, or when a standby-snapshot endpoint runs past EOF. A tail with
// a complete fixed header but invalid TERM fields may return an error instead of
// being dropped.
//
// Unrecognised type words are returned as opaque entries. NULL_VALUE removed
// slots are skipped.
//
// Returns ErrTermRecordingIDInvalid, ErrTermRecordingIDMismatch,
// ErrEndpointTooLong, or ErrStandbyEndpointEmpty when decode validation fails.
// Read failures return the os.ReadFile error unchanged.
func Reload(path string) ([]Entry, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	entries, err := decodeEntries(buf)
	if err != nil {
		return nil, err
	}
	sortEntries(entries)
	return entries, nil
}

func decodeEntries(buf []byte) ([]Entry, error) {
	entries, _, _, err := decodeEntriesAndCount(buf)
	return entries, err
}

// decodeEntriesAndCount decodes buf and returns the next file-order entry
// ordinal: one increment per logical on-disk record (decoded entry or NULL
// removed slot), independent of encoded byte span. Seeds nextEntryIndex on Open.
// Differs from len(entries) when the file holds removed slots.
//
// Byte consumption follows alignedEntryLength; standby entries may exceed
// recordAlignment. The loop enters when at least 49 bytes remain so the type
// word is readable. NULL slots advance consumed by recordAlignment even when
// fewer than 64 bytes remain, so a truncated null record still bumps the entry
// ordinal. Standby endpoint overruns and sub-49-byte tails break without
// counting.
func decodeEntriesAndCount(buf []byte) ([]Entry, int32, int64, error) {
	buffer := atomicbuffer.Wrap(buf)
	entries := make([]Entry, 0, len(buf)/recordAlignment)
	consumed := 0
	entryOrdinal := int32(0)
	termRecordingID := nullValue

	for consumed+endpointOffset < len(buf) {
		rawType := buffer.GetInt32(consumed + entryTypeOffset)
		if rawType == nullEntryType {
			consumed += recordAlignment
			entryOrdinal++
			continue
		}
		entryType := rawType &^ entryTypeInvalidFlag
		valid := (rawType & entryTypeInvalidFlag) == 0

		endpoint := ""
		if hasEndpoint(entryType) {
			epOffset := consumed + endpointOffset
			epDataOffset := epOffset + atomicbuffer.StringASCIIHeaderLength
			if epDataOffset > len(buf) {
				break
			}
			epLen := int(buffer.GetInt32(epOffset))
			if epLen < 0 || epLen > maxEndpointLen {
				return nil, 0, nullValue, ErrEndpointTooLong
			}
			if epLen == 0 {
				return nil, 0, nullValue, ErrStandbyEndpointEmpty
			}
			if epDataOffset+epLen > len(buf) {
				break
			}
			endpoint = string(buf[epDataOffset : epDataOffset+epLen])
		}

		entry := Entry{
			RecordingID:         buffer.GetInt64(consumed + recordingIDOffset),
			LeadershipTermID:    buffer.GetInt64(consumed + leadershipTermIDOffset),
			TermBaseLogPosition: buffer.GetInt64(consumed + termBaseLogPositionOffset),
			LogPosition:         buffer.GetInt64(consumed + logPositionOffset),
			Timestamp:           buffer.GetInt64(consumed + timestampOffset),
			ServiceID:           buffer.GetInt32(consumed + serviceIDOffset),
			Type:                entryType,
			Valid:               valid,
			ArchiveEndpoint:     endpoint,
			Position:            int64(consumed),
			Index:               entryOrdinal,
		}

		if entryType == EntryTypeTerm {
			if entry.RecordingID == nullValue {
				return nil, 0, nullValue, ErrTermRecordingIDInvalid
			}
			if termRecordingID == nullValue {
				termRecordingID = entry.RecordingID
			} else if entry.RecordingID != termRecordingID {
				return nil, 0, nullValue, ErrTermRecordingIDMismatch
			}
		}
		entries = append(entries, entry)

		consumed += alignedEntryLength(entryType, endpoint)
		entryOrdinal++
	}

	return entries, entryOrdinal, termRecordingID, nil
}

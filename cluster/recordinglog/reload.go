package recordinglog

import (
	"errors"
	"os"

	"github.com/warrenguy/heron/agrona/atomic"
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
)

const nullValue int64 = -1

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
	buffer := atomic.Wrap(buf)
	entries := make([]Entry, 0, len(buf)/recordAlignment)
	consumed := 0
	index := int32(0)
	termRecordingID := nullValue

	for consumed+endpointOffset < len(buf) {
		rawType := buffer.GetInt32(consumed + entryTypeOffset)
		if rawType == nullEntryType {
			consumed += recordAlignment
			index++
			continue
		}
		entryType := rawType &^ entryTypeInvalidFlag
		valid := (rawType & entryTypeInvalidFlag) == 0

		endpoint := ""
		if hasEndpoint(entryType) {
			epOffset := consumed + endpointOffset
			epDataOffset := epOffset + atomic.StringASCIIHeaderLength
			if epDataOffset > len(buf) {
				break
			}
			epLen := int(buffer.GetInt32(epOffset))
			if epLen < 0 || epLen > maxEndpointLen {
				return nil, ErrEndpointTooLong
			}
			if epLen == 0 {
				return nil, ErrStandbyEndpointEmpty
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
			Index:               index,
		}

		if entryType == EntryTypeTerm {
			if entry.RecordingID == nullValue {
				return nil, ErrTermRecordingIDInvalid
			}
			if termRecordingID == nullValue {
				termRecordingID = entry.RecordingID
			} else if entry.RecordingID != termRecordingID {
				return nil, ErrTermRecordingIDMismatch
			}
		}
		entries = append(entries, entry)

		consumed += alignedEntryLength(entryType, endpoint)
		index++
	}

	return entries, nil
}

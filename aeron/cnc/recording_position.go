package cnc

import (
	"errors"

	"github.com/warrenguy/heron/atomicbuffer"
)

var errRecordingPositionSourceIdentityTooLong = errors.New(
	"cnc: recording position source identity exceeds the counter key",
)

const (
	// ArchiveRecordingPositionCounterTypeID mirrors Java
	// AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID.
	//
	// A mismatch makes FindRecordingPositionCounter never match a real counter written by
	// the media driver.
	ArchiveRecordingPositionCounterTypeID int32 = 100

	// ArchiveControlSessionsCounterTypeID is the type id of the archive counter
	// that counts active control sessions. Matches
	// AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID.
	ArchiveControlSessionsCounterTypeID int32 = 102

	// Binary counter key layout for a recording-position counter: recordingId int64 at
	// offset 0, sessionId int32 at 8, sourceIdentityLength int32 at 12, sourceIdentity
	// bytes at 16, then archiveId int64 after the source identity.
	recordingPositionRecordingIDKeyOffset          = 0
	recordingPositionSessionIDKeyOffset            = 8
	recordingPositionSourceIdentityLengthKeyOffset = 12
	recordingPositionSourceIdentityKeyOffset       = 16
	// The archive id follows the source identity, so both must fit in the key.
	recordingPositionMaxSourceIdentityLength = CounterMetadataMaxKeyLength -
		recordingPositionSourceIdentityKeyOffset - atomicbuffer.SizeInt64
)

// FindRecordingPositionCounter scans counters for the active recording-position counter
// whose binary key records recordingID and returns its bound value slot.
//
// archiveID filters by the archive id encoded after the source identity; pass NullValue
// to match any archive.
//
// Returns false when no active counter matches.
func (reader *CountersReader) FindRecordingPositionCounter(
	recordingID int64,
	archiveID int64,
) (
	CounterSlot,
	bool,
) {
	if reader == nil {
		return CounterSlot{}, false
	}

	for counterID := int32(0); counterID <= reader.maxCounterID; counterID++ {
		metadataOffset := counterMetadataOffset(counterID)
		state := reader.metadata.GetInt32Volatile(metadataOffset + CounterMetadataStateOffset)
		if state == CounterRecordUnused {
			break
		}
		if reader.recordingPositionCounterMatchesAt(metadataOffset, state, recordingID, archiveID) {
			return NewCounterSlot(counterID, reader.values), true
		}
	}

	return CounterSlot{}, false
}

// isRecordingPositionCounter reports whether counterID is the active
// recording-position counter of recordingID on archiveID. NullValue archiveID
// matches any archive.
func (reader *CountersReader) isRecordingPositionCounter(
	counterID int32,
	recordingID int64,
	archiveID int64,
) bool {
	if reader == nil || counterID < 0 || counterID > reader.maxCounterID {
		return false
	}
	metadataOffset := counterMetadataOffset(counterID)
	state := reader.metadata.GetInt32Volatile(metadataOffset + CounterMetadataStateOffset)
	return reader.recordingPositionCounterMatchesAt(metadataOffset, state, recordingID, archiveID)
}

// recordingPositionCounterMatchesAt reports whether the counter record at
// metadataOffset with state is an allocated recording-position counter whose
// key names recordingID and archiveID. NullValue archiveID matches any archive.
func (reader *CountersReader) recordingPositionCounterMatchesAt(
	metadataOffset int,
	state int32,
	recordingID int64,
	archiveID int64,
) bool {
	if !reader.counterAllocatedOfTypeAt(
		metadataOffset,
		state,
		ArchiveRecordingPositionCounterTypeID,
	) {
		return false
	}

	keyOffset := metadataOffset + CounterMetadataKeyOffset
	if reader.metadata.GetInt64(keyOffset+recordingPositionRecordingIDKeyOffset) != recordingID {
		return false
	}
	if archiveID == NullValue {
		return true
	}
	// The CnC metadata buffer is driver-mmap'd and trusted, so the key read needs
	// no bounds guard on sourceIdentityLength (matching upstream).
	sourceIdentityLength := reader.metadata.GetInt32(
		keyOffset + recordingPositionSourceIdentityLengthKeyOffset,
	)
	archiveIDOffset := keyOffset +
		recordingPositionSourceIdentityKeyOffset +
		int(sourceIdentityLength)
	return reader.metadata.GetInt64(archiveIDOffset) == archiveID
}

// RecordingPositionCache reads the recording-position counter of one
// recording on one archive.
//
// Behaviour: caches one counter slot. Each read first revalidates the cached
// slot for the recording id and the archive id. A miss rescans the counters.
//
// Concurrency: invoker-only.
type RecordingPositionCache struct {
	reader    *CountersReader
	archiveID int64

	// cached reports whether cachedSlot holds the counter of
	// cachedRecordingID.
	cached            bool
	cachedRecordingID int64
	cachedSlot        CounterSlot
}

// NewRecordingPositionCache returns a cache over reader for the archive with
// archiveID. NullValue matches any archive. With a nil reader, every read
// misses.
func NewRecordingPositionCache(reader *CountersReader, archiveID int64) *RecordingPositionCache {
	return &RecordingPositionCache{reader: reader, archiveID: archiveID}
}

// RecordingPosition returns the counter value for recordingID and true. It
// returns 0 and false when no active counter matches.
func (c *RecordingPositionCache) RecordingPosition(recordingID int64) (int64, bool) {
	cachedSlotLive := c.cached &&
		c.cachedRecordingID == recordingID &&
		c.reader.isRecordingPositionCounter(c.cachedSlot.CounterID(), recordingID, c.archiveID)
	if cachedSlotLive {
		return c.cachedSlot.Get(), true
	}
	slot, ok := c.reader.FindRecordingPositionCounter(recordingID, c.archiveID)
	c.cached = ok
	if !ok {
		return 0, false
	}
	c.cachedRecordingID = recordingID
	c.cachedSlot = slot
	return slot.Get(), true
}

// WriteRecordingPositionCounterRecord seeds a recording-position counter for tests using
// driver-parity key layout (recordingId at key+0, sessionId at key+8, sourceIdentityLength
// at key+12, sourceIdentity at key+16, archiveId after the source identity).
func WriteRecordingPositionCounterRecord(
	meta []byte,
	values []byte,
	counterID int32,
	state int32,
	typeID int32,
	recordingID int64,
	sessionID int32,
	sourceIdentity string,
	archiveID int64,
	value int64,
) error {
	if len(sourceIdentity) > recordingPositionMaxSourceIdentityLength {
		return errRecordingPositionSourceIdentityTooLong
	}

	metaBuffer := atomicbuffer.Wrap(meta)
	valuesBuffer := atomicbuffer.Wrap(values)
	return writeCounterRecord(
		metaBuffer,
		valuesBuffer,
		counterID,
		state,
		typeID,
		value,
		func(metaOffset, _ int) error {
			keyOffset := metaOffset + CounterMetadataKeyOffset
			metaBuffer.PutInt64(keyOffset+recordingPositionRecordingIDKeyOffset, recordingID)
			metaBuffer.PutInt32(keyOffset+recordingPositionSessionIDKeyOffset, sessionID)
			metaBuffer.PutInt32(
				keyOffset+recordingPositionSourceIdentityLengthKeyOffset,
				//nolint:gosec // G115: checked against recordingPositionMaxSourceIdentityLength above
				int32(len(sourceIdentity)),
			)
			metaBuffer.PutBytes(
				keyOffset+recordingPositionSourceIdentityKeyOffset,
				[]byte(sourceIdentity),
			)
			metaBuffer.PutInt64(
				keyOffset+recordingPositionSourceIdentityKeyOffset+len(sourceIdentity),
				archiveID,
			)
			return nil
		},
	)
}

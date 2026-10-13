package cnc_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
)

func seedRecordingPositionCounter(
	t *testing.T,
	meta []byte,
	values []byte,
	counterID int32,
	typeID int32,
	recordingID int64,
	sessionID int32,
	sourceIdentity string,
	archiveID int64,
	value int64,
) {
	t.Helper()
	require.NoError(t, cnc.WriteRecordingPositionCounterRecord(
		meta, values, counterID, cnc.CounterRecordAllocated, typeID,
		recordingID, sessionID, sourceIdentity, archiveID, value,
	))
}

func TestArchiveRecordingPositionCounterTypeID(t *testing.T) {
	// Fixed against the Aeron archive recording-position counter type id. The
	// literal detects silent drift; synthetic tests seed counters with the
	// named constant and would pass regardless of its value.
	require.Equal(t, int32(100), cnc.ArchiveRecordingPositionCounterTypeID)
}

func TestFindRecordingPositionCounter(t *testing.T) {
	const (
		recordingID       int64  = 909
		sessionID         int32  = 7
		archiveID         int64  = 424242
		positionValue     int64  = 65536
		matchingCounterID int32  = 1
		sourceIdentity    string = "aeron:ipc-source-identity"
	)

	meta, values := syntheticCountersBuffers(t, 4)

	// Same recordingId but a different counter type: skipped by the type filter.
	seedRecordingPositionCounter(
		t, meta, values, 0, cnc.ClientHeartbeatTypeID,
		recordingID, sessionID, "other-source", archiveID, 1,
	)
	// The recording-position counter with a non-zero sourceIdentityLength.
	seedRecordingPositionCounter(
		t, meta, values, matchingCounterID, cnc.ArchiveRecordingPositionCounterTypeID,
		recordingID, sessionID, sourceIdentity, archiveID, positionValue,
	)
	// A recording-position counter for a different recording.
	seedRecordingPositionCounter(
		t, meta, values, 2, cnc.ArchiveRecordingPositionCounterTypeID,
		recordingID+1, sessionID, sourceIdentity, archiveID, 42,
	)
	// Counter 3 stays unused (state 0): the scan stop sentinel.

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	slot, ok := reader.FindRecordingPositionCounter(recordingID, archiveID)
	require.True(t, ok)
	require.Equal(t, matchingCounterID, slot.CounterID())
	require.Equal(t, positionValue, slot.Get())

	_, ok = reader.FindRecordingPositionCounter(recordingID, archiveID+1)
	require.False(t, ok)

	slot, ok = reader.FindRecordingPositionCounter(recordingID, cnc.NullValue)
	require.True(t, ok)
	require.Equal(t, matchingCounterID, slot.CounterID())

	_, ok = reader.FindRecordingPositionCounter(recordingID+2, archiveID)
	require.False(t, ok)
}

func TestFindRecordingPositionCounter_skipsReclaimedSlot(t *testing.T) {
	const (
		recordingID       int64 = 909
		sessionID         int32 = 7
		archiveID         int64 = 424242
		positionValue     int64 = 65536
		matchingCounterID int32 = 1
	)

	meta, values := syntheticCountersBuffers(t, 3)

	// A reclaimed slot with a matching recording id: the scan must skip it, not stop.
	require.NoError(t, cnc.WriteRecordingPositionCounterRecord(
		meta, values, 0, cnc.CounterRecordReclaimed, cnc.ArchiveRecordingPositionCounterTypeID,
		recordingID, sessionID, "reclaimed-source", archiveID, 111,
	))
	// The matching allocated counter after the reclaimed slot.
	seedRecordingPositionCounter(
		t, meta, values, matchingCounterID, cnc.ArchiveRecordingPositionCounterTypeID,
		recordingID, sessionID, "aeron:ipc-source", archiveID, positionValue,
	)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	slot, ok := reader.FindRecordingPositionCounter(recordingID, archiveID)
	require.True(t, ok)
	require.Equal(t, matchingCounterID, slot.CounterID())
	require.Equal(t, positionValue, slot.Get())
}

func TestFindRecordingPositionCounter_nilReader(t *testing.T) {
	var reader *cnc.CountersReader
	_, ok := reader.FindRecordingPositionCounter(1, cnc.NullValue)
	require.False(t, ok)
}

func TestWriteRecordingPositionCounterRecord_boundsSourceIdentity(t *testing.T) {
	write := func(sourceIdentity string) ([]byte, error) {
		meta, values := syntheticCountersBuffers(t, 1)
		err := cnc.WriteRecordingPositionCounterRecord(
			meta, values, 0, cnc.CounterRecordAllocated,
			cnc.ArchiveRecordingPositionCounterTypeID, 1, 1, sourceIdentity, 1, 0,
		)
		return meta, err
	}
	_, err := write(strings.Repeat("a", 88))
	require.NoError(t, err)

	meta, err := write(strings.Repeat("a", 89))
	require.EqualError(t, err, "cnc: recording position source identity exceeds the counter key")
	require.Zero(t, atomicbuffer.Wrap(meta).GetInt32(cnc.CounterMetadataTypeIDOffset))
}

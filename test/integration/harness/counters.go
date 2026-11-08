//go:build integration

package harness

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron/cnc"
)

// PublicationLimitCounterValue resolves the driver publisher-limit counter for
// registrationID via FindHeartbeatCounter and DriverPublisherLimitTypeID.
// registrationID must be the publication OriginalRegistrationID.
func PublicationLimitCounterValue(
	t testing.TB,
	reader *cnc.CountersReader,
	registrationID int64,
) (int32, int64) {
	t.Helper()

	limitID, ok := reader.FindHeartbeatCounter(registrationID, cnc.DriverPublisherLimitTypeID)
	if !ok {
		require.FailNow(
			t,
			fmt.Sprintf(
				"publication limit counter not found for registration %d",
				registrationID,
			),
		)
		return cnc.NullCounterID, 0
	}
	driverLimit, err := reader.CounterValue(limitID)
	require.NoError(t, err)
	return limitID, driverLimit
}

// DriverCounters reads counters from a media driver's CnC file by type id.
//
// Behaviour:
//   - Caches the first allocated counter id of each type.
//   - Rescans when the cached record no longer carries the type.
//   - Does not filter by registration id. When one driver allocates a type more
//     than once, the scan returns the lowest id.
//
// Concurrency: not safe for concurrent calls.
//
// Ownership: owns the mapping. Does not own the driver.
type DriverCounters struct {
	mapped *cnc.MappedCncFile
	reader *cnc.CountersReader
	byType map[int32]int32
}

// OpenDriverCounters maps the CnC file under aeronDir.
//
// Returns an error when the directory does not hold a readable CnC file.
func OpenDriverCounters(aeronDir string) (*DriverCounters, error) {
	mapped, err := cnc.MapCncFile(filepath.Join(aeronDir, cnc.FileName))
	if err != nil {
		return nil, err
	}

	reader, err := cnc.NewCountersReader(
		mapped.CountersMetaDataBuffer(),
		mapped.CountersValuesBuffer(),
	)
	if err != nil {
		_ = mapped.Close()
		return nil, err
	}

	return &DriverCounters{
		mapped: mapped,
		reader: reader,
		byType: make(map[int32]int32),
	}, nil
}

// Value returns the value of the first allocated counter of typeID.
//
// Returns false when no counter of that type is allocated, or when the receiver
// is nil or closed.
func (c *DriverCounters) Value(typeID int32) (int64, bool) {
	if c == nil || c.reader == nil {
		return 0, false
	}

	id, found := c.counterIDOfType(typeID)
	if !found {
		return 0, false
	}
	value, err := c.reader.CounterValue(id)
	return value, err == nil
}

// counterIDOfType returns the counter id for typeID.
// Prefers the cached id while that record stays allocated with typeID.
// A reclaimed or retyped cached record forces one rescan.
func (c *DriverCounters) counterIDOfType(typeID int32) (int32, bool) {
	cached, ok := c.byType[typeID]
	if ok && c.reader.IsCounterAllocatedOfType(cached, typeID) {
		return cached, true
	}

	id, found := c.reader.FindCounterOfType(typeID)
	if !found {
		delete(c.byType, typeID)
		return cnc.NullCounterID, false
	}
	c.byType[typeID] = id
	return id, true
}

// Close unmaps the CnC file. Close is idempotent.
func (c *DriverCounters) Close() error {
	if c == nil || c.mapped == nil {
		return nil
	}

	mapped := c.mapped
	c.mapped = nil
	c.reader = nil
	return mapped.Close()
}

const (
	// recordingPositionRecordingIDOffset is the key offset of the recording
	// id. Matches RecordingPos.RECORDING_ID_OFFSET.
	recordingPositionRecordingIDOffset = 0

	// recordingPositionSessionIDOffset is the key offset of the session id.
	// Matches RecordingPos.SESSION_ID_OFFSET.
	recordingPositionSessionIDOffset = 8
)

// RecordingPosition is one archive recording-position counter.
type RecordingPosition struct {
	RecordingID int64
	Position    int64
}

// RecordingPosition returns the value of the RecordingPos counter for
// recordingID on archiveID.
//
// Returns 0, false when the receiver is nil or closed, or when no active
// counter matches.
func (c *DriverCounters) RecordingPosition(archiveID, recordingID int64) (int64, bool) {
	if c == nil || c.reader == nil {
		return 0, false
	}

	slot, ok := c.reader.FindRecordingPositionCounter(recordingID, archiveID)
	if !ok {
		return 0, false
	}

	return slot.Get(), true
}

// HasClient reports whether the driver holds a client heartbeat counter for
// clientID.
func (c *DriverCounters) HasClient(clientID int64) bool {
	if c == nil || c.reader == nil {
		return false
	}

	_, ok := c.reader.FindHeartbeatCounter(clientID, cnc.ClientHeartbeatTypeID)
	return ok
}

// RecordingPositionsBySession returns every allocated recording-position
// counter whose key holds sessionID, in ascending counter id order.
//
// Returns nil when the receiver is nil or closed. Does not filter on
// archive id.
func (c *DriverCounters) RecordingPositionsBySession(sessionID int32) []RecordingPosition {
	if c == nil || c.reader == nil {
		return nil
	}

	limit := c.reader.CounterValuesBuffer().Capacity() / cnc.CounterValueLength
	var out []RecordingPosition

	for counterID := range int32(limit) {
		if !c.reader.IsCounterAllocatedOfType(
			counterID,
			cnc.ArchiveRecordingPositionCounterTypeID,
		) {
			continue
		}

		key, ok := c.reader.CounterKey(counterID)
		if !ok {
			continue
		}

		gotSession := int32(binary.LittleEndian.Uint32(key[recordingPositionSessionIDOffset:]))
		if gotSession != sessionID {
			continue
		}

		position, _ := c.reader.CounterValue(counterID)
		out = append(out, RecordingPosition{
			RecordingID: int64(binary.LittleEndian.Uint64(key[recordingPositionRecordingIDOffset:])),
			Position:    position,
		})
	}
	return out
}

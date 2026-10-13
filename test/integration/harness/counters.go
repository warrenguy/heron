//go:build integration

package harness

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
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
//   - Maps the CnC file once.
//   - On first lookup of a type id, finds the first allocated counter of that
//     type and caches the id.
//   - Later lookups revalidate the cached id.
//   - Rescans only when the cached record no longer carries the type.
//   - Does not filter by registration id. When one driver allocates a type more
//     than once, the lowest id wins.
//
// Concurrency: invoker-only. Every method runs on the caller goroutine.
//
// Lifetime: Close unmaps the file. Close is idempotent.
//
// Ownership: owns the mapping. Does not own the driver.
type DriverCounters struct {
	mapped *cnc.MappedCncFile
	reader *cnc.CountersReader
	byType map[int32]int32
}

// OpenDriverCounters maps the CnC file under aeronDir.
//
// Returns an error when the directory holds no readable CnC file.
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

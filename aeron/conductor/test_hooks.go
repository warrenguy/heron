//go:build testhooks

// Cross-package test hooks for conductor internals.
//
// Package-level functions mirror export_test.go receiver methods where both exist
// so conductor_test can use the latter without symbol conflicts.
package conductor

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func WriteMinimalLogFileForTest(tb testing.TB, path string) {
	tb.Helper()

	const termLength = logbuffer.TermMinLength
	const pageSize = logbuffer.PageMinSize
	computed, err := logbuffer.ComputeLogLength(termLength, pageSize)
	require.NoError(tb, err)

	buf := make([]byte, computed)
	metaStart := logbuffer.MetaDataOffset(termLength)
	meta := aeronatomic.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])
	meta.PutInt32Release(logbuffer.OffsetTermLength, termLength)
	meta.PutInt32(logbuffer.OffsetPageSize, pageSize)
	meta.PutInt32(logbuffer.OffsetMTULength, 1408)
	meta.PutInt32(logbuffer.OffsetInitialTermID, 7)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	require.NoError(tb, os.WriteFile(path, buf, 0o644))
}

func SetCloseLingerNsForTest(conductor *Conductor, lingerNs int64) {
	conductor.config.CloseLingerNs = lingerNs
}

func assertInvokeImageHandlerUnderWriteLock(savedDepth int32) {
	if savedDepth <= 0 {
		panic("conductor: invokeImageHandler called without resource write lock")
	}
}

// RegisterSubscriptionForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses InsertSubscriptionForTest in export_test.go instead.
func RegisterSubscriptionForTest(
	conductor *Conductor,
	registrationID int64,
	sub SubscriptionHandle,
) {
	registerSubscriptionHook(conductor, registrationID, sub)
}

// AcquireLogBuffersForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).AcquireLogBuffersForTest in export_test.go instead.
func AcquireLogBuffersForTest(
	conductor *Conductor,
	logFileName string,
) (*logbuffer.LogBuffers, error) {
	return acquireLogBuffersHook(conductor, logFileName)
}

// RegisterPublicationForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses InsertPublicationForTest in export_test.go instead.
func RegisterPublicationForTest(
	conductor *Conductor,
	registrationID int64,
	publication PublicationHandle,
) {
	registerPublicationHook(conductor, registrationID, publication)
}

// RemovePublicationForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).RemovePublicationForTest in export_test.go instead.
func RemovePublicationForTest(conductor *Conductor, registrationID int64) {
	removePublicationHook(conductor, registrationID)
}

// RemoveSubscriptionForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).RemoveSubscriptionForTest in export_test.go instead.
func RemoveSubscriptionForTest(conductor *Conductor, registrationID int64) {
	removeSubscriptionHook(conductor, registrationID)
}

// PublicationCountForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).PublicationCountForTest in export_test.go instead.
func PublicationCountForTest(conductor *Conductor) int {
	return publicationCountHook(conductor)
}

// SubscriptionCountForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).SubscriptionCountForTest in export_test.go instead.
func SubscriptionCountForTest(conductor *Conductor) int {
	return subscriptionCountHook(conductor)
}

func LogBuffersRegistryForTest(conductor *Conductor) *LogBuffersRegistry {
	return logBuffersRegistryHook(conductor)
}

func LogBuffersRegistryRefCountForTest(
	registry *LogBuffersRegistry,
	logFileName string,
) int32 {
	return logBuffersRegistryRefCountHook(registry, logFileName)
}

func LogBuffersRegistryContainsForTest(
	registry *LogBuffersRegistry,
	logFileName string,
) bool {
	return logBuffersRegistryContainsHook(registry, logFileName)
}

func LogBuffersRegistryLingerScheduledForTest(registry *LogBuffersRegistry) int {
	return logBuffersRegistryLingerScheduledHook(registry)
}

func LogBuffersRegistryCountScheduledLingersForTest(registry *LogBuffersRegistry) int {
	return logBuffersRegistryCountScheduledLingersHook(registry)
}

func LingerDeadlineNsForTest(registry *LogBuffersRegistry, logFileName string) int64 {
	return lingerDeadlineNsHook(registry, logFileName)
}

func (conductor *Conductor) SetLingerSweepBlockerForTest(blocker func()) {
	setLingerSweepBlockerHook(conductor, blocker)
}

func (conductor *Conductor) SetCountersReaderForTest(reader *cnc.CountersReader) {
	setCountersReaderHook(conductor, reader)
}

func (conductor *Conductor) EventsAdapterForTest() *DriverEventsAdapter {
	return eventsAdapterHook(conductor)
}

func (adapter *DriverEventsAdapter) CopyReceiverForTest() *CopyBroadcastReceiver {
	return copyReceiverHook(adapter)
}

// NewConductorUnchecked builds a conductor with synthetic driver I/O for tests.
func NewConductorUnchecked(
	config Config,
	countersReader *cnc.CountersReader,
) (*Conductor, error) {
	return newConductorUncheckedHook(config, countersReader)
}

func (conductor *Conductor) SetReceivedCorrelationIDForTest(correlationID int64) {
	setReceivedCorrelationIDHook(conductor, correlationID)
}

func (conductor *Conductor) CompletePendingForTest(
	correlationID int64,
	int32Value int32,
	err error,
) error {
	return completePendingHook(conductor, correlationID, int32Value, err)
}

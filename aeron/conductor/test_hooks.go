//go:build testhooks

// Cross-package test hooks for conductor internals.
//
// Package-level functions mirror export_test.go receiver methods where both exist
// so conductor_test can use the latter without symbol conflicts.
package conductor

import (
	"github.com/warrenguy/heron/aeron/cnc"
)

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

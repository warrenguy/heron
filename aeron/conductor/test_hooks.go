//go:build testhooks

package conductor

import (
	"fmt"

	"github.com/warrenguy/heron/aeron/cnc"
)

func assertInvokeImageCallbackUnderWriteLock(savedDepth int32) {
	if savedDepth <= 0 {
		panic(fmt.Sprintf(
			"conductor: invokeImageCallback called without resource write lock (depth=%d)",
			savedDepth,
		))
	}
}

// RegisterSubscriptionForTest is for cross-package tests behind the testhooks tag.
func RegisterSubscriptionForTest(
	conductor *Conductor,
	registrationID int64,
	sub SubscriptionHandle,
) {
	registerSubscriptionHook(conductor, registrationID, sub)
}

// RegisterPublicationForTest is for cross-package tests behind the testhooks tag.
// conductor_test uses InsertPublicationForTest in export_test.go instead.
func RegisterPublicationForTest(
	conductor *Conductor,
	registrationID int64,
	publication PublicationHandle,
) {
	registerPublicationHook(conductor, registrationID, publication)
}

// RemovePublicationForTest is for cross-package tests behind the testhooks tag.
// conductor_test uses (*Conductor).RemovePublicationForTest in export_test.go instead.
func RemovePublicationForTest(conductor *Conductor, registrationID int64) {
	removePublicationHook(conductor, registrationID)
}

// RemoveSubscriptionForTest is for cross-package tests behind the testhooks tag.
// conductor_test uses (*Conductor).RemoveSubscriptionForTest in export_test.go instead.
func RemoveSubscriptionForTest(conductor *Conductor, registrationID int64) {
	removeSubscriptionHook(conductor, registrationID)
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

func (conductor *Conductor) SetCountersReaderForTest(reader *cnc.CountersReader) {
	setCountersReaderHook(conductor, reader)
}

func (conductor *Conductor) EventsAdapterForTest() *DriverEventsAdapter {
	return eventsAdapterHook(conductor)
}

func (adapter *DriverEventsAdapter) CopyReceiverForTest() *CopyBroadcastReceiver {
	return copyReceiverHook(adapter)
}

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

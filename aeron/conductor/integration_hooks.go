//go:build integration

package conductor

import "github.com/warrenguy/heron/aeron/cnc"

func RegisterSubscriptionForTest(
	conductor *Conductor,
	registrationID int64,
	sub SubscriptionHandle,
) {
	registerSubscriptionHook(conductor, registrationID, sub)
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

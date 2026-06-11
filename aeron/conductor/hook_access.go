//go:build testhooks

package conductor

import (
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const testRingDataCapacity = 4096

func registerSubscriptionHook(
	conductor *Conductor,
	registrationID int64,
	sub SubscriptionHandle,
) {
	conductor.underResourceWriteLock(func() {
		conductor.insertSubscription(registrationID, sub)
	})
}

func registerPublicationHook(
	conductor *Conductor,
	registrationID int64,
	publication PublicationHandle,
) {
	conductor.underResourceWriteLock(func() {
		conductor.insertPublication(registrationID, publication)
	})
}

func removePublicationHook(conductor *Conductor, registrationID int64) {
	conductor.underResourceWriteLock(func() {
		conductor.removePublication(registrationID)
	})
}

func removeSubscriptionHook(conductor *Conductor, registrationID int64) {
	conductor.underResourceWriteLock(func() {
		conductor.removeSubscription(registrationID)
	})
}

func logBuffersRegistryHook(conductor *Conductor) *LogBuffersRegistry {
	return conductor.logBuffersRegistry
}

func logBuffersRegistryRefCountHook(
	registry *LogBuffersRegistry,
	logFileName string,
) int32 {
	return registry.refCount(logFileName)
}

func logBuffersRegistryContainsHook(
	registry *LogBuffersRegistry,
	logFileName string,
) bool {
	return registry.contains(logFileName)
}

func logBuffersRegistryLingerScheduledHook(registry *LogBuffersRegistry) int {
	return registry.lingerScheduledCount()
}

func invokeLingerSweepBlockerForTest(conductor *Conductor) {
	if conductor.lingerSweepBlocker != nil {
		conductor.lingerSweepBlocker()
	}
}

func setLingerSweepBlockerHook(conductor *Conductor, blocker func()) {
	conductor.lingerSweepBlocker = blocker
}

func logBuffersRegistryCountScheduledLingersHook(registry *LogBuffersRegistry) int {
	return registry.countScheduledLingers()
}

func setCountersReaderHook(conductor *Conductor, reader *cnc.CountersReader) {
	conductor.countersReader = reader
}

func eventsAdapterHook(conductor *Conductor) *DriverEventsAdapter {
	return conductor.eventsAdapter
}

func copyReceiverHook(adapter *DriverEventsAdapter) *CopyBroadcastReceiver {
	return adapter.copyReceiver
}

func setReceivedCorrelationIDHook(conductor *Conductor, correlationID int64) {
	conductor.receivedCorrelationID = correlationID
}

func completePendingHook(
	conductor *Conductor,
	correlationID int64,
	int32Value int32,
	err error,
) error {
	if err == nil {
		if !conductor.OnNextAvailableSessionID(correlationID, int32Value) {
			return ErrPendingNotFound
		}
		return nil
	}
	registration, ok := conductor.pending.Get(correlationID)
	if !ok {
		return ErrPendingNotFound
	}
	registration.Err = err
	conductor.receivedCorrelationID = correlationID
	return nil
}

func newConductorUncheckedHook(
	config Config,
	countersReader *cnc.CountersReader,
) (*Conductor, error) {
	if countersReader == nil {
		meta := make([]byte, cnc.CounterMetadataLength)
		values := make([]byte, cnc.CounterValueLength)
		var err error
		countersReader, err = cnc.NewCountersReader(meta, values)
		if err != nil {
			return nil, err
		}
	}

	ringBuffer := aeronatomic.Wrap(
		make([]byte, ringbuffer.TotalBufferLength(testRingDataCapacity)),
	)
	ring, err := ringbuffer.NewManyToOneRingBuffer(ringBuffer)
	if err != nil {
		return nil, err
	}
	if config.EpochClock != nil {
		ring.SetConsumerHeartbeatTime(config.EpochClock())
	}

	broadcastBuffer := aeronatomic.Wrap(
		make([]byte, broadcast.TotalBufferLength(testRingDataCapacity)),
	)
	broadcastReceiver, err := broadcast.NewBroadcastReceiver(broadcastBuffer)
	if err != nil {
		return nil, err
	}

	return NewConductor(
		NewDriverProxy(ring, config.ClientID),
		broadcastReceiver,
		countersReader,
		config,
	), nil
}

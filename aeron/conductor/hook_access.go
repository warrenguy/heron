//go:build testhooks

package conductor

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/agrona/broadcast"
	"github.com/warrenguy/heron/agrona/ringbuffer"
	"github.com/warrenguy/heron/internal/testhook"
)

const testRingDataCapacity = 4096

var lingerSweepBlockers = testhook.NewRegistry[*Conductor, func()]()

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

func acquireLogBuffersHook(
	conductor *Conductor,
	logFileName string,
) (*logbuffer.LogBuffers, error) {
	var buffers *logbuffer.LogBuffers
	var err error
	conductor.underResourceWriteLock(func() {
		buffers, err = conductor.acquireLogBuffers(logFileName)
	})
	return buffers, err
}

func logBuffersRegistryHook(conductor *Conductor) *LogBuffersRegistry {
	return conductor.logBuffersRegistry
}

func publicationCountHook(conductor *Conductor) int {
	return withResourceRLock(conductor, conductor.publicationCount)
}

func subscriptionCountHook(conductor *Conductor) int {
	return withResourceRLock(conductor, conductor.subscriptionCount)
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

func logBuffersRegistryCountScheduledLingersHook(registry *LogBuffersRegistry) int {
	return registry.countScheduledLingers()
}

func lingerDeadlineNsHook(registry *LogBuffersRegistry, logFileName string) int64 {
	return registry.lingerDeadlineNs(logFileName)
}

func setLingerSweepBlockerHook(conductor *Conductor, blocker func()) {
	if conductor == nil {
		return
	}
	if blocker == nil {
		lingerSweepBlockers.Delete(conductor)
		return
	}
	lingerSweepBlockers.Store(conductor, blocker)
}

func invokeLingerSweepBlocker(conductor *Conductor) {
	if conductor == nil {
		return
	}
	blocker := lingerSweepBlockers.Load(conductor)
	if blocker != nil {
		blocker()
	}
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

func publicationReadyHook(conductor *Conductor, event PublicationBuffersReady) {
	conductor.handlePublicationReady(event)
}

func exclusivePublicationReadyHook(conductor *Conductor, event PublicationBuffersReady) {
	conductor.handleExclusivePublicationReady(event)
}

func completePendingHook(
	conductor *Conductor,
	correlationID int64,
	int32Value int32,
	err error,
) error {
	if err == nil {
		if conductor.NextSessionID(correlationID, int32Value) {
			return nil
		}
		return ErrPendingNotFound
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
		reader, err := cnc.NewCountersReader(meta, values)
		if err != nil {
			return nil, err
		}
		countersReader = reader
	}

	ringBuffer := atomic.Wrap(
		make([]byte, ringbuffer.TotalBufferLength(testRingDataCapacity)),
	)
	ring, err := ringbuffer.NewManyToOneRingBuffer(ringBuffer)
	if err != nil {
		return nil, err
	}
	if config.EpochClock != nil {
		ring.SetConsumerHeartbeatTime(config.EpochClock())
	}

	broadcastBuffer := atomic.Wrap(
		make([]byte, broadcast.TotalBufferLength(testRingDataCapacity)),
	)
	broadcastReceiver, err := broadcast.NewBroadcastReceiver(broadcastBuffer)
	if err != nil {
		return nil, err
	}

	return newConductor(
		NewDriverProxy(ring, config.ClientID),
		broadcastReceiver,
		countersReader,
		config,
	), nil
}

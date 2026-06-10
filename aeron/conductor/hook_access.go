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
	conductor.subscriptions[registrationID] = sub
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

func setCountersReaderHook(conductor *Conductor, reader *cnc.CountersReader) {
	conductor.countersReader = reader
}

func eventsAdapterHook(conductor *Conductor) *DriverEventsAdapter {
	return conductor.eventsAdapter
}

func copyReceiverHook(adapter *DriverEventsAdapter) *CopyBroadcastReceiver {
	return adapter.copyReceiver
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

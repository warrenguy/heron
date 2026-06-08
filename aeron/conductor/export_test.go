package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const conductorTestRingDataCapacity = 4096

func (adapter *DriverEventsAdapter) CopyReceiverForTest() *CopyBroadcastReceiver {
	return adapter.copyReceiver
}

func (conductor *Conductor) DriverProxyForTest() *DriverProxy {
	return conductor.driverProxy
}

func (conductor *Conductor) EventsAdapterForTest() *DriverEventsAdapter {
	return conductor.eventsAdapter
}

func (conductor *Conductor) CountersReaderForTest() *cnc.CountersReader {
	return conductor.countersReader
}

func (conductor *Conductor) SetHeartbeatCounterIDForTest(counterID int32) {
	conductor.heartbeatCounterID = counterID
}

func (conductor *Conductor) TimeOfLastKeepAliveNsForTest() int64 {
	return conductor.timeOfLastKeepAliveNs
}

func (conductor *Conductor) StoreClientHeartbeatForTest(nowNs, nowMs int64) int {
	return conductor.storeClientHeartbeat(nowNs, nowMs)
}

func (conductor *Conductor) SetCountersReaderForTest(reader *cnc.CountersReader) {
	conductor.countersReader = reader
}

func (conductor *Conductor) CompleteNextSessionIDForTest(
	correlationID int64,
	nextSessionID int32,
) bool {
	return conductor.completeNextSessionID(correlationID, nextSessionID)
}

func (conductor *Conductor) SetTerminatingForTest(terminating bool) {
	conductor.isTerminating = terminating
}

func (proxy *DriverProxy) Ring() *ringbuffer.ManyToOneRingBuffer {
	return proxy.ring
}

func NewCopyBroadcastReceiverWithScratchLength(
	receiver *broadcast.BroadcastReceiver,
	scratchLength int,
) *CopyBroadcastReceiver {
	return newCopyBroadcastReceiver(receiver, scratchLength)
}

func (proxy *DriverProxy) OfferCorrelatedForTest(msgTypeID int) error {
	return proxy.offerCorrelated(msgTypeID, false)
}

func (proxy *DriverProxy) OfferCorrelatedSilentForTest(msgTypeID int) error {
	return proxy.offerCorrelated(msgTypeID, true)
}

func (proxy *DriverProxy) OfferCorrelatedBodyForTest(
	msgTypeID int,
	bodyLength int,
	correlationID int64,
	silent bool,
	afterEncode func(encodedOffset int) error,
) error {
	return proxy.offerCorrelatedBody(msgTypeID, bodyLength, correlationID, silent, afterEncode)
}

func (proxy *DriverProxy) OfferBodyForTest(
	msgTypeID int,
	bodyLength int,
	silent bool,
	encode func(encodedOffset int) error,
) error {
	return proxy.offerBody(msgTypeID, bodyLength, silent, encode)
}

func NewConductorUnchecked(
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
		make([]byte, ringbuffer.TotalBufferLength(conductorTestRingDataCapacity)),
	)
	ring, err := ringbuffer.NewManyToOneRingBuffer(ringBuffer)
	if err != nil {
		return nil, err
	}
	if config.EpochClock != nil {
		ring.SetConsumerHeartbeatTime(config.EpochClock())
	}

	broadcastBuffer := aeronatomic.Wrap(
		make([]byte, broadcast.TotalBufferLength(conductorTestRingDataCapacity)),
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

func NewConductorForTest(
	tb testing.TB,
	config Config,
	countersReader *cnc.CountersReader,
) *Conductor {
	tb.Helper()

	conductorInstance, err := NewConductorUnchecked(config, countersReader)
	require.NoError(tb, err)
	return conductorInstance
}

func DecodeTrailingStringASCIIForTest(
	buf aeronatomic.AtomicBuffer,
	offset, bodyLength, stringOffset int,
	wrapped error,
) (string, error) {
	return decodeTrailingStringASCII(buf, offset, bodyLength, stringOffset, wrapped)
}

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

func NewConductorForTest(
	tb testing.TB,
	config Config,
	countersReader *cnc.CountersReader,
) *Conductor {
	tb.Helper()

	if countersReader == nil {
		meta := make([]byte, cnc.CounterMetadataLength)
		values := make([]byte, cnc.CounterValueLength)
		var err error
		countersReader, err = cnc.NewCountersReader(meta, values)
		require.NoError(tb, err)
	}

	ring := newConductorTestRing(tb)
	if config.EpochClock != nil {
		ring.SetConsumerHeartbeatTime(config.EpochClock())
	}

	_, broadcastReceiver := newConductorTestBroadcastPair(tb)
	return NewConductor(
		NewDriverProxy(ring, config.ClientID),
		broadcastReceiver,
		countersReader,
		config,
	)
}

func newConductorTestBuffer(tb testing.TB, totalLength int) aeronatomic.AtomicBuffer {
	tb.Helper()

	return aeronatomic.Wrap(make([]byte, totalLength))
}

func newConductorTestRing(tb testing.TB) *ringbuffer.ManyToOneRingBuffer {
	tb.Helper()

	buffer := newConductorTestBuffer(tb, ringbuffer.TotalBufferLength(conductorTestRingDataCapacity))
	ring, err := ringbuffer.NewManyToOneRingBuffer(buffer)
	require.NoError(tb, err)
	return ring
}

func DecodeTrailingStringASCIIForTest(
	buf aeronatomic.AtomicBuffer,
	offset, bodyLength, stringOffset int,
	wrapped error,
) (string, error) {
	return decodeTrailingStringASCII(buf, offset, bodyLength, stringOffset, wrapped)
}

func newConductorTestBroadcastPair(
	tb testing.TB,
) (*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver) {
	tb.Helper()

	buffer := newConductorTestBuffer(tb, broadcast.TotalBufferLength(conductorTestRingDataCapacity))
	receiver, err := broadcast.NewBroadcastReceiver(buffer)
	require.NoError(tb, err)
	transmitter, err := broadcast.NewBroadcastTransmitter(buffer)
	require.NoError(tb, err)
	return transmitter, receiver
}

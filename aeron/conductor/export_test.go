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
	afterEncode func(encodedOffset int),
) error {
	return proxy.offerCorrelatedBody(
		msgTypeID,
		bodyLength,
		correlationID,
		silent,
		afterEncode,
	)
}

func (proxy *DriverProxy) EncodeCommitCorrelatedAtForTest(
	claimOffset int,
	encodeOffset int,
) error {
	return proxy.encodeCommitCorrelatedAt(claimOffset, encodeOffset)
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

func newConductorTestRing(tb testing.TB) *ringbuffer.ManyToOneRingBuffer {
	tb.Helper()

	bufferBytes := make([]byte, ringbuffer.TotalBufferLength(conductorTestRingDataCapacity))
	buffer := aeronatomic.Wrap(bufferBytes)
	ring, err := ringbuffer.NewManyToOneRingBuffer(buffer)
	require.NoError(tb, err)
	return ring
}

func newConductorTestBroadcastPair(
	tb testing.TB,
) (*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver) {
	tb.Helper()

	bufferBytes := make([]byte, broadcast.TotalBufferLength(conductorTestRingDataCapacity))
	buffer := aeronatomic.Wrap(bufferBytes)
	receiver, err := broadcast.NewBroadcastReceiver(buffer)
	require.NoError(tb, err)
	transmitter, err := broadcast.NewBroadcastTransmitter(buffer)
	require.NoError(tb, err)
	return transmitter, receiver
}

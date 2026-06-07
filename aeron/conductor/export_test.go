package conductor

import (
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

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

func (proxy *DriverProxy) EncodeCommitCorrelatedAtForTest(
	claimOffset int,
	encodeOffset int,
) error {
	return proxy.encodeCommitCorrelatedAt(claimOffset, encodeOffset)
}

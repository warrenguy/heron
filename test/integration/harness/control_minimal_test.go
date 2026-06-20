//go:build !integration

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/agrona/record"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

const (
	controlMinimalTestDataCapacity = 4096
	controlMinimalWrapPadding      = 22
)

func TestScanPendingToDriverMessages_incompleteRecordAtHead(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	conductor.NewDriverProxy(ring, clientID).ClientClose()

	buffer.PutInt32(record.LengthOffset(0), 0)

	decoded, ok, err := findPendingClientClose(ring, clientID)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestScanPendingToDriverMessages_clientCloseSkipsOtherClient(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const otherClientID int64 = 3
	const clientID int64 = 7
	conductor.NewDriverProxy(ring, otherClientID).ClientClose()
	conductor.NewDriverProxy(ring, clientID).ClientClose()

	decoded, ok, err := findPendingClientClose(ring, clientID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}, decoded)
}

func TestScanPendingToDriverMessages_clientClose(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	conductor.NewDriverProxy(ring, clientID).ClientClose()

	decoded, ok, err := findPendingClientClose(ring, clientID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}, decoded)
}

func TestScanPendingToDriverMessages_addPublication_incompleteRecordAtHead(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))

	buffer.PutInt32(record.LengthOffset(0), 0)

	decoded, ok, err := findPendingAddPublication(ring, want)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestScanPendingToDriverMessages_addPublicationSkipsOtherClient(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const otherClientID int64 = 3
	want := controlMinimalTestAddPublicationWant()
	otherWant := conductor.PublicationMessage{
		ClientID:      otherClientID,
		CorrelationID: want.CorrelationID,
		StreamID:      want.StreamID,
		Channel:       want.Channel,
	}
	require.NoError(t, conductor.NewDriverProxy(ring, otherClientID).AddPublication(
		otherWant.CorrelationID, otherWant.StreamID, otherWant.Channel,
	))
	require.NoError(t, conductor.NewDriverProxy(ring, want.ClientID).AddPublication(
		want.CorrelationID, want.StreamID, want.Channel,
	))

	decoded, ok, err := findPendingAddPublication(ring, want)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func TestScanPendingToDriverMessages_addPublication(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))

	decoded, ok, err := findPendingAddPublication(ring, want)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func TestFindOfferedClientCloseAt_beforeOffer(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	decoded, ok, err := findOfferedClientCloseAt(ring, clientID, ring.ProducerPosition())
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestFindOfferedClientCloseAt_clientClose(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	producerBefore := ring.ProducerPosition()
	conductor.NewDriverProxy(ring, clientID).ClientClose()

	decoded, ok, err := findOfferedClientCloseAt(ring, clientID, producerBefore)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}, decoded)
}

func TestFindConsumedClientCloseAt_clientClose(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	conductor.NewDriverProxy(ring, clientID).ClientClose()
	advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())

	decoded, ok, err := findConsumedClientCloseAt(
		ring,
		clientID,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		-1,
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}, decoded)
}

func TestFindConsumedClientCloseAt_clearedAfterConsume(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	conductor.NewDriverProxy(ring, clientID).ClientClose()
	advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())
	buffer.SetBytes(0, conductor.CorrelatedRingRecordLength(), 0)

	decoded, ok, err := findConsumedClientCloseAt(
		ring,
		clientID,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		consumerBefore,
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}, decoded)
}

func TestFindConsumedClientCloseAt_clearedWithoutProof(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	conductor.NewDriverProxy(ring, clientID).ClientClose()
	advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())
	buffer.SetBytes(0, conductor.CorrelatedRingRecordLength(), 0)

	decoded, ok, err := findConsumedClientCloseAt(
		ring,
		clientID,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		-1,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestFindConsumedClientCloseAt_clientKeepaliveRejected(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	require.NoError(t, conductor.NewDriverProxy(ring, clientID).ClientKeepalive())
	advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())

	decoded, ok, err := findConsumedClientCloseAt(
		ring,
		clientID,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		-1,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestObserveClientClose_offeredWithOlderPendingMessage(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	proxy := conductor.NewDriverProxy(ring, clientID)
	require.NoError(t, proxy.ClientKeepalive())

	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()
	proxy.ClientClose()

	proven := int64(-1)
	decoded, ok, err := observeClientClose(
		ring,
		clientID,
		consumerBefore,
		producerBefore,
		&proven,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}, decoded)
	require.Equal(t, producerBefore, proven)
}

func TestObserveClientClose(t *testing.T) {
	tests := []struct {
		name  string
		setup func(
			t *testing.T,
			ring *ringbuffer.ManyToOneRingBuffer,
			buffer atomic.AtomicBuffer,
			consumerBefore int64,
			producerBefore int64,
		)
		seedProven func(consumerBefore, producerBefore int64) int64
	}{
		{
			name: "offered",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				_ atomic.AtomicBuffer,
				_, _ int64,
			) {
				t.Helper()
				conductor.NewDriverProxy(ring, 7).ClientClose()
			},
		},
		{
			name: "consumed",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				consumerBefore, _ int64,
			) {
				t.Helper()
				conductor.NewDriverProxy(ring, 7).ClientClose()
				advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())
			},
		},
		{
			name: "clearedAfterConsume",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				consumerBefore, _ int64,
			) {
				t.Helper()
				conductor.NewDriverProxy(ring, 7).ClientClose()
				advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())
				buffer.SetBytes(0, conductor.CorrelatedRingRecordLength(), 0)
			},
			seedProven: func(consumerBefore, producerBefore int64) int64 {
				return consumerBefore
			},
		},
		{
			name: "clearedAfterCloseOnlyOffer",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				consumerBefore, producerBefore int64,
			) {
				t.Helper()
				require.Equal(t, consumerBefore, producerBefore)
				conductor.NewDriverProxy(ring, 7).ClientClose()
				advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())
				buffer.SetBytes(0, conductor.CorrelatedRingRecordLength(), 0)
			},
		},
	}

	const clientID int64 = 7
	want := conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ring, buffer := newControlMinimalTestRingBuffer(t)
			consumerBefore := ring.ConsumerPosition()
			producerBefore := ring.ProducerPosition()
			testCase.setup(t, ring, buffer, consumerBefore, producerBefore)

			proven := int64(-1)
			if testCase.seedProven != nil {
				proven = testCase.seedProven(consumerBefore, producerBefore)
			}
			decoded, ok, err := observeClientClose(
				ring,
				clientID,
				consumerBefore,
				producerBefore,
				&proven,
			)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, want, decoded)
		})
	}
}

func TestFindConsumedClientCloseAt_clearedKeepaliveRejected(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	require.NoError(t, conductor.NewDriverProxy(ring, clientID).ClientKeepalive())
	advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())
	buffer.SetBytes(0, conductor.CorrelatedRingRecordLength(), 0)

	decoded, ok, err := findConsumedClientCloseAt(
		ring,
		clientID,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		-1,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestFindOfferedAddPublicationAt_beforeOffer(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	decoded, ok, err := findOfferedAddPublicationAt(ring, ring.ProducerPosition(), want)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestFindOfferedAddPublicationAt_addPublication(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()

	producerBefore := ring.ProducerPosition()
	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))

	decoded, ok, err := findOfferedAddPublicationAt(ring, producerBefore, want)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func TestFindOfferedAddPublicationAt_wrapPadding(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want, _, producerBefore, _, _ := offerAddPublicationAcrossWrap(t, ring, buffer)

	var matchedPosition int64
	decoded, ok, err := findOfferedAt(
		ring,
		producerBefore,
		addPublicationObserveSpec(want),
		&matchedPosition,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
	require.Equal(t, producerBefore+controlMinimalWrapPadding, matchedPosition)
	require.Equal(t, 0, int(matchedPosition)&(ring.Capacity()-1))
}

func TestFindPendingAddPublication_wrapPadding(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want, _, _, _, _ := offerAddPublicationAcrossWrap(t, ring, buffer)

	decoded, ok, err := findPendingAddPublication(ring, want)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func controlMinimalTestAddPublicationWant() conductor.PublicationMessage {
	return conductor.PublicationMessage{
		ClientID:      7,
		CorrelationID: 42,
		StreamID:      1001,
		Channel:       "aeron:ipc",
	}
}

func controlMinimalTestAddPublicationRecordLength(channel string) int {
	return ringRecordLength(conductor.PublicationMessageBodyLength(channel))
}

func TestFindConsumedAddPublicationAt_addPublication(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	recordLength := controlMinimalTestAddPublicationRecordLength(want.Channel)
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))
	advanceControlMinimalConsumerByRecordLength(
		t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
	)

	decoded, ok, err := findConsumedAddPublicationAt(
		ring,
		want,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		-1,
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func TestFindConsumedAddPublicationAt_wrapPadding(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want, consumerBefore, producerBefore, _, producerAfter := offerAddPublicationAcrossWrap(
		t, ring, buffer,
	)
	advanceControlMinimalConsumerTo(t, ring, buffer, producerAfter)

	decoded, ok, err := findConsumedAddPublicationAt(
		ring,
		want,
		consumerBefore,
		producerAfter,
		producerBefore,
		producerAfter,
		consumerBefore,
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func TestFindConsumedAddPublicationAt_wrapPadding_clearedAfterConsume(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want, consumerBefore, producerBefore, recordLength, producerAfter := offerAddPublicationAcrossWrap(
		t, ring, buffer,
	)
	advanceControlMinimalConsumerTo(t, ring, buffer, producerAfter)
	clearRecordAtIndex0(buffer, recordLength)

	decoded, ok, err := findConsumedAddPublicationAt(
		ring,
		want,
		consumerBefore,
		producerAfter,
		producerBefore,
		producerAfter,
		consumerBefore,
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func TestFindConsumedAddPublicationAt_clearedAfterConsume(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	recordLength := controlMinimalTestAddPublicationRecordLength(want.Channel)
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))
	advanceControlMinimalConsumerByRecordLength(
		t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
	)
	buffer.SetBytes(0, recordLength, 0)

	decoded, ok, err := findConsumedAddPublicationAt(
		ring,
		want,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		consumerBefore,
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func TestFindConsumedAddPublicationAt_clearedWithoutProof(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	recordLength := controlMinimalTestAddPublicationRecordLength(want.Channel)
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))
	advanceControlMinimalConsumerByRecordLength(
		t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
	)
	buffer.SetBytes(0, recordLength, 0)

	decoded, ok, err := findConsumedAddPublicationAt(
		ring,
		want,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		-1,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestFindConsumedAddPublicationAt_clientKeepaliveRejected(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.ClientKeepalive())
	advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())

	decoded, ok, err := findConsumedAddPublicationAt(
		ring,
		want,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		-1,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestFindConsumedAddPublicationAt_keepaliveAndAddPublicationRejected(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.ClientKeepalive())
	require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))

	producerAfter := ring.ProducerPosition()
	advanceControlMinimalConsumerTo(t, ring, buffer, producerAfter)

	decoded, ok, err := findConsumedAddPublicationAt(
		ring,
		want,
		consumerBefore,
		producerAfter,
		producerBefore,
		producerAfter,
		-1,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func TestObserveAddPublication_offeredWithOlderPendingMessage(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const (
		clientID      int64 = 7
		correlationID int64 = 42
		streamID      int32 = 1001
		channel             = "aeron:ipc"
	)
	want := conductor.PublicationMessage{
		ClientID:      clientID,
		CorrelationID: correlationID,
		StreamID:      streamID,
		Channel:       channel,
	}

	proxy := conductor.NewDriverProxy(ring, clientID)
	require.NoError(t, proxy.ClientKeepalive())

	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()
	require.NoError(t, proxy.AddPublication(correlationID, streamID, channel))

	proven := int64(-1)
	decoded, ok, err := observeAddPublication(
		ring,
		want,
		consumerBefore,
		producerBefore,
		&proven,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
	require.Equal(t, producerBefore, proven)
}

func TestObserveAddPublication_wrapPadding(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want, consumerBefore, producerBefore, _, _ := offerAddPublicationAcrossWrap(t, ring, buffer)

	proven := int64(-1)
	decoded, ok, err := observeAddPublication(
		ring,
		want,
		consumerBefore,
		producerBefore,
		&proven,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
	require.Equal(t, producerBefore+controlMinimalWrapPadding, proven)
	require.Equal(t, 0, int(proven)&(ring.Capacity()-1))
}

func TestObserveAddPublication_wrapPadding_clearedAfterConsume(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want, consumerBefore, producerBefore, recordLength, producerAfter := offerAddPublicationAcrossWrap(
		t, ring, buffer,
	)

	proven := int64(-1)
	decoded, ok, err := observeAddPublication(
		ring,
		want,
		consumerBefore,
		producerBefore,
		&proven,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
	require.Equal(t, producerBefore+controlMinimalWrapPadding, proven)

	advanceControlMinimalConsumerTo(t, ring, buffer, producerAfter)
	clearRecordAtIndex0(buffer, recordLength)

	decoded, ok, err = observeAddPublication(
		ring,
		want,
		consumerBefore,
		producerBefore,
		&proven,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, decoded)
}

func TestAdvanceRingScanPosition(t *testing.T) {
	tests := []struct {
		name          string
		messageTypeID int
		recordLength  int
		want          int64
	}{
		{
			name:          "padding raw length",
			messageTypeID: ringbuffer.PaddingMsgTypeID,
			recordLength:  controlMinimalWrapPadding,
			want:          controlMinimalWrapPadding,
		},
		{
			name:          "padding aligned boundary",
			messageTypeID: ringbuffer.PaddingMsgTypeID,
			recordLength:  ringbuffer.HeaderLength,
			want:          int64(ringbuffer.HeaderLength),
		},
		{
			name:          "normal unaligned body",
			messageTypeID: conductor.AddPublication,
			recordLength:  41,
			want:          48,
		},
		{
			name:          "normal already aligned",
			messageTypeID: conductor.AddPublication,
			recordLength:  48,
			want:          48,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, advanceRingScanPosition(tc.messageTypeID, tc.recordLength))
		})
	}
}

func TestObserveAddPublication(t *testing.T) {
	want := controlMinimalTestAddPublicationWant()
	recordLength := controlMinimalTestAddPublicationRecordLength(want.Channel)

	tests := []struct {
		name  string
		setup func(
			t *testing.T,
			ring *ringbuffer.ManyToOneRingBuffer,
			buffer atomic.AtomicBuffer,
			consumerBefore int64,
			producerBefore int64,
		)
		seedProven func(consumerBefore, producerBefore int64) int64
	}{
		{
			name: "offered",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				_ atomic.AtomicBuffer,
				_, _ int64,
			) {
				t.Helper()
				proxy := conductor.NewDriverProxy(ring, want.ClientID)
				require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))
			},
		},
		{
			name: "pending",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				_ atomic.AtomicBuffer,
				_, _ int64,
			) {
				t.Helper()
				proxy := conductor.NewDriverProxy(ring, want.ClientID)
				require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))
			},
			seedProven: func(consumerBefore, _ int64) int64 {
				return consumerBefore
			},
		},
		{
			name: "consumed",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				consumerBefore, _ int64,
			) {
				t.Helper()
				proxy := conductor.NewDriverProxy(ring, want.ClientID)
				require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)
			},
		},
		{
			name: "clearedAfterConsume",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				consumerBefore, _ int64,
			) {
				t.Helper()
				proxy := conductor.NewDriverProxy(ring, want.ClientID)
				require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)
				buffer.SetBytes(0, recordLength, 0)
			},
			seedProven: func(consumerBefore, _ int64) int64 {
				return consumerBefore
			},
		},
		{
			name: "clearedAfterAddPublicationOnlyOffer",
			setup: func(
				t *testing.T,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				consumerBefore, producerBefore int64,
			) {
				t.Helper()
				require.Equal(t, consumerBefore, producerBefore)
				proxy := conductor.NewDriverProxy(ring, want.ClientID)
				require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)
				buffer.SetBytes(0, recordLength, 0)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ring, buffer := newControlMinimalTestRingBuffer(t)
			consumerBefore := ring.ConsumerPosition()
			producerBefore := ring.ProducerPosition()
			testCase.setup(t, ring, buffer, consumerBefore, producerBefore)

			proven := int64(-1)
			if testCase.seedProven != nil {
				proven = testCase.seedProven(consumerBefore, producerBefore)
			}
			decoded, ok, err := observeAddPublication(
				ring,
				want,
				consumerBefore,
				producerBefore,
				&proven,
			)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, want, decoded)
		})
	}
}

func TestFindConsumedAddPublicationAt_clearedKeepaliveRejected(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	want := controlMinimalTestAddPublicationWant()
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()

	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.ClientKeepalive())
	advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())
	buffer.SetBytes(0, conductor.CorrelatedRingRecordLength(), 0)

	decoded, ok, err := findConsumedAddPublicationAt(
		ring,
		want,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		-1,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, decoded)
}

func seedControlMinimalRingNearWrap(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer atomic.AtomicBuffer,
) {
	t.Helper()

	position := int64(controlMinimalTestDataCapacity - controlMinimalWrapPadding)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(controlMinimalTestDataCapacity), position)
	buffer.PutInt64Release(ringbuffer.HeadIndex(controlMinimalTestDataCapacity), position)
	buffer.PutInt64Release(ringbuffer.TailIndex(controlMinimalTestDataCapacity), position)
}

func requireWrapPaddingAddPublicationLayout(
	t *testing.T,
	buffer atomic.AtomicBuffer,
	producerBefore int64,
	recordLength int,
	producerAfter int64,
) {
	t.Helper()

	require.Equal(
		t,
		producerBefore+int64(recordLength)+controlMinimalWrapPadding,
		producerAfter,
	)
	require.Equal(
		t,
		int32(ringbuffer.PaddingMsgTypeID),
		buffer.GetInt32(record.TypeOffset(int(producerBefore))),
	)
	require.Equal(
		t,
		int32(conductor.AddPublication),
		buffer.GetInt32(record.TypeOffset(0)),
	)
}

func offerAddPublicationAcrossWrap(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer atomic.AtomicBuffer,
) (
	want conductor.PublicationMessage,
	consumerBefore int64,
	producerBefore int64,
	recordLength int,
	producerAfter int64,
) {
	t.Helper()

	seedControlMinimalRingNearWrap(t, ring, buffer)

	want = controlMinimalTestAddPublicationWant()
	consumerBefore = ring.ConsumerPosition()
	producerBefore = ring.ProducerPosition()
	recordLength = controlMinimalTestAddPublicationRecordLength(want.Channel)

	proxy := conductor.NewDriverProxy(ring, want.ClientID)
	require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))

	producerAfter = ring.ProducerPosition()
	requireWrapPaddingAddPublicationLayout(t, buffer, producerBefore, recordLength, producerAfter)
	return want, consumerBefore, producerBefore, recordLength, producerAfter
}

func advanceControlMinimalConsumerTo(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer atomic.AtomicBuffer,
	position int64,
) {
	t.Helper()

	// Harness consumed-path tests advance head by producer delta directly.
	// Real ring.Read() advances padding by aligned length (24 for 22-byte padding).
	buffer.PutInt64Release(ringbuffer.HeadIndex(ring.Capacity()), position)
}

func clearRecordAtIndex0(buffer atomic.AtomicBuffer, recordLength int) {
	buffer.SetBytes(0, recordLength, 0)
}

func advanceControlMinimalConsumerByRecordLength(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer atomic.AtomicBuffer,
	consumerBefore, producerAfter int64,
	recordLength int,
) {
	t.Helper()

	require.Equal(
		t,
		producerAfter,
		consumerBefore+int64(recordLength),
	)
	buffer.PutInt64Release(
		ringbuffer.HeadIndex(ring.Capacity()),
		producerAfter,
	)
}

func advanceControlMinimalConsumer(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer atomic.AtomicBuffer,
	consumerBefore, producerAfter int64,
) {
	t.Helper()
	advanceControlMinimalConsumerByRecordLength(
		t,
		ring,
		buffer,
		consumerBefore,
		producerAfter,
		conductor.CorrelatedRingRecordLength(),
	)
}

func newControlMinimalTestRingBuffer(
	t *testing.T,
) (*ringbuffer.ManyToOneRingBuffer, atomic.AtomicBuffer) {
	t.Helper()

	buf := make([]byte, ringbuffer.TotalBufferLength(controlMinimalTestDataCapacity))
	buffer := atomic.Wrap(buf)
	ring, err := ringbuffer.NewManyToOneRingBuffer(buffer)
	require.NoError(t, err)
	return ring, buffer
}

func controlMinimalTestRemoveIDs() (clientID, registrationID int64) {
	return 7, 9001
}

type removeTestOps struct {
	name        string
	publication bool
}

var removeTestOpsVariants = []removeTestOps{
	{name: "removePublication", publication: true},
	{name: "removeSubscription", publication: false},
}

func offerRemoveForTest(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, correlationID, registrationID int64,
	publication bool,
) {
	t.Helper()

	proxy := conductor.NewDriverProxy(ring, clientID)
	if publication {
		require.NoError(t, proxy.RemovePublication(correlationID, registrationID, false))
		return
	}
	require.NoError(t, proxy.RemoveSubscription(correlationID, registrationID))
}

func offerRemovePublicationForTest(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, correlationID, registrationID int64,
) {
	t.Helper()
	offerRemoveForTest(t, ring, clientID, correlationID, registrationID, true)
}

func offerRemoveSubscriptionForTest(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, correlationID, registrationID int64,
) {
	t.Helper()
	offerRemoveForTest(t, ring, clientID, correlationID, registrationID, false)
}

func controlMinimalTestRemoveRecordLength(publication bool) int {
	if publication {
		return ringRecordLength(conductor.RemovePublicationBodyLength)
	}
	return ringRecordLength(conductor.RemoveMessageBodyLength)
}

func controlMinimalTestRemovePublicationRecordLength() int {
	return controlMinimalTestRemoveRecordLength(true)
}

func controlMinimalTestRemoveSubscriptionRecordLength() int {
	return controlMinimalTestRemoveRecordLength(false)
}

func (ops removeTestOps) offer(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, correlationID, registrationID int64,
) {
	t.Helper()
	offerRemoveForTest(t, ring, clientID, correlationID, registrationID, ops.publication)
}

func (ops removeTestOps) recordLength() int {
	return controlMinimalTestRemoveRecordLength(ops.publication)
}

func (ops removeTestOps) typeID() int32 {
	if ops.publication {
		return int32(conductor.RemovePublication)
	}
	return int32(conductor.RemoveSubscription)
}

func (ops removeTestOps) wireBody(clientID, correlationID, registrationID int64) []byte {
	if ops.publication {
		return conductor.RemovePublicationMessageBytes(
			clientID,
			correlationID,
			registrationID,
			false,
		)
	}
	return conductor.RemoveMessageBytes(clientID, correlationID, registrationID)
}

func (ops removeTestOps) findPending(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
) (conductor.RemoveMessage, int64, bool, error) {
	if ops.publication {
		decoded, ok, err := findPendingRemovePublication(ring, clientID, registrationID)
		return decoded.RemoveMessage, decoded.CorrelationID, ok, err
	}
	decoded, ok, err := findPendingRemoveSubscription(ring, clientID, registrationID)
	return decoded, decoded.CorrelationID, ok, err
}

func (ops removeTestOps) findOfferedAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID, producerBefore int64,
) (conductor.RemoveMessage, bool, error) {
	if ops.publication {
		decoded, ok, err := findOfferedRemovePublicationAt(ring, clientID, registrationID, producerBefore)
		return decoded.RemoveMessage, ok, err
	}
	return findOfferedRemoveSubscriptionAt(ring, clientID, registrationID, producerBefore)
}

func (ops removeTestOps) findConsumedAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
	consumerBefore, consumerAfter, producerBefore, producerAfter int64,
	provenPosition int64,
) (conductor.RemoveMessage, bool, error) {
	if ops.publication {
		decoded, ok, err := findConsumedRemovePublicationAt(
			ring,
			clientID,
			registrationID,
			consumerBefore,
			consumerAfter,
			producerBefore,
			producerAfter,
			provenPosition,
			false,
		)
		return decoded.RemoveMessage, ok, err
	}
	return findConsumedRemoveSubscriptionAt(
		ring,
		clientID,
		registrationID,
		consumerBefore,
		consumerAfter,
		producerBefore,
		producerAfter,
		provenPosition,
		false,
	)
}

func (ops removeTestOps) observe(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
	consumerBefore, producerBefore int64,
	proven *int64,
) (conductor.RemoveMessage, bool, error) {
	if ops.publication {
		decoded, ok, err := observeOnRing(
			ring,
			consumerBefore,
			producerBefore,
			proven,
			removePublicationObserveSpec(clientID, registrationID),
		)
		return decoded.RemoveMessage, ok, err
	}
	return observeOnRing(
		ring,
		consumerBefore,
		producerBefore,
		proven,
		removeSubscriptionObserveSpec(clientID, registrationID),
	)
}

func (ops removeTestOps) assertZero(t *testing.T, got conductor.RemoveMessage) {
	t.Helper()
	if ops.publication {
		require.Zero(t, conductor.RemovePublicationMessage{RemoveMessage: got})
		return
	}
	require.Zero(t, got)
}

func (ops removeTestOps) findOfferedAtWrap(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID, producerBefore int64,
	matchedPosition *int64,
) (conductor.RemoveMessage, bool, error) {
	if ops.publication {
		decoded, ok, err := findOfferedAt(
			ring,
			producerBefore,
			removePublicationObserveSpec(clientID, registrationID),
			matchedPosition,
		)
		return decoded.RemoveMessage, ok, err
	}
	return findOfferedAt(
		ring,
		producerBefore,
		removeSubscriptionObserveSpec(clientID, registrationID),
		matchedPosition,
	)
}

func (ops removeTestOps) offerAcrossWrap(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer atomic.AtomicBuffer,
) removeAcrossWrapOffer {
	t.Helper()

	seedControlMinimalRingNearWrap(t, ring, buffer)

	clientID, registrationID := controlMinimalTestRemoveIDs()
	consumerBefore := ring.ConsumerPosition()
	producerBefore := ring.ProducerPosition()
	recordLength := ops.recordLength()

	ops.offer(t, ring, clientID, 42, registrationID)

	producerAfter := ring.ProducerPosition()
	requireWrapPaddingRemoveLayout(
		t,
		buffer,
		producerBefore,
		recordLength,
		producerAfter,
		ops.typeID(),
	)
	return removeAcrossWrapOffer{
		ClientID:       clientID,
		RegistrationID: registrationID,
		ConsumerBefore: consumerBefore,
		ProducerBefore: producerBefore,
		ProducerAfter:  producerAfter,
		RecordLength:   recordLength,
	}
}

func requireRemoveMessageIdentity(
	t *testing.T,
	wantClientID, wantRegistrationID int64,
	got conductor.RemoveMessage,
) {
	t.Helper()

	require.Equal(t, wantClientID, got.ClientID)
	require.Equal(t, wantRegistrationID, got.RegistrationID)
}

func TestRemovePublicationAndRemoveSubscription_recordLengthsDistinct(t *testing.T) {
	require.NotEqual(
		t,
		controlMinimalTestRemovePublicationRecordLength(),
		controlMinimalTestRemoveSubscriptionRecordLength(),
	)
}

func TestScanPendingToDriverMessages_remove(t *testing.T) {
	const otherClientID int64 = 3

	for _, ops := range removeTestOpsVariants {
		t.Run(ops.name, func(t *testing.T) {
			clientID, registrationID := controlMinimalTestRemoveIDs()

			t.Run("incompleteRecordAtHead", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				ops.offer(t, ring, clientID, 42, registrationID)
				buffer.PutInt32(record.LengthOffset(0), 0)

				decoded, _, ok, err := ops.findPending(ring, clientID, registrationID)
				require.NoError(t, err)
				require.False(t, ok)
				ops.assertZero(t, decoded)
			})

			t.Run("skipsOtherClient", func(t *testing.T) {
				ring, _ := newControlMinimalTestRingBuffer(t)
				ops.offer(t, ring, otherClientID, 1, registrationID)
				ops.offer(t, ring, clientID, 2, registrationID)

				decoded, _, ok, err := ops.findPending(ring, clientID, registrationID)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
			})

			t.Run("skipsWrongRegistrationID", func(t *testing.T) {
				ring, _ := newControlMinimalTestRingBuffer(t)
				ops.offer(t, ring, clientID, 1, registrationID+1)
				ops.offer(t, ring, clientID, 2, registrationID)

				decoded, _, ok, err := ops.findPending(ring, clientID, registrationID)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
			})

			t.Run("found", func(t *testing.T) {
				ring, _ := newControlMinimalTestRingBuffer(t)
				ops.offer(t, ring, clientID, 42, registrationID)

				decoded, _, ok, err := ops.findPending(ring, clientID, registrationID)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
			})
		})
	}
}

func TestFindOfferedRemoveAt(t *testing.T) {
	for _, ops := range removeTestOpsVariants {
		t.Run(ops.name, func(t *testing.T) {
			clientID, registrationID := controlMinimalTestRemoveIDs()

			t.Run("beforeOffer", func(t *testing.T) {
				ring, _ := newControlMinimalTestRingBuffer(t)
				decoded, ok, err := ops.findOfferedAt(
					ring, clientID, registrationID, ring.ProducerPosition(),
				)
				require.NoError(t, err)
				require.False(t, ok)
				ops.assertZero(t, decoded)
			})

			t.Run("found", func(t *testing.T) {
				ring, _ := newControlMinimalTestRingBuffer(t)
				producerBefore := ring.ProducerPosition()
				ops.offer(t, ring, clientID, 42, registrationID)

				decoded, ok, err := ops.findOfferedAt(ring, clientID, registrationID, producerBefore)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
			})
		})
	}
}

func TestFindConsumedRemoveAt(t *testing.T) {
	for _, ops := range removeTestOpsVariants {
		t.Run(ops.name, func(t *testing.T) {
			clientID, registrationID := controlMinimalTestRemoveIDs()
			recordLength := ops.recordLength()

			t.Run("found", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				consumerBefore := ring.ConsumerPosition()
				producerBefore := ring.ProducerPosition()

				ops.offer(t, ring, clientID, 42, registrationID)
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)

				decoded, ok, err := ops.findConsumedAt(
					ring,
					clientID,
					registrationID,
					consumerBefore,
					ring.ConsumerPosition(),
					producerBefore,
					ring.ProducerPosition(),
					-1,
				)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
			})

			t.Run("clearedAfterConsume", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				consumerBefore := ring.ConsumerPosition()
				producerBefore := ring.ProducerPosition()

				ops.offer(t, ring, clientID, 42, registrationID)
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)
				buffer.SetBytes(0, recordLength, 0)

				decoded, ok, err := ops.findConsumedAt(
					ring,
					clientID,
					registrationID,
					consumerBefore,
					ring.ConsumerPosition(),
					producerBefore,
					ring.ProducerPosition(),
					consumerBefore,
				)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
			})

			t.Run("clearedWithoutProof", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				consumerBefore := ring.ConsumerPosition()
				producerBefore := ring.ProducerPosition()

				ops.offer(t, ring, clientID, 42, registrationID)
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)
				buffer.SetBytes(0, recordLength, 0)

				decoded, ok, err := ops.findConsumedAt(
					ring,
					clientID,
					registrationID,
					consumerBefore,
					ring.ConsumerPosition(),
					producerBefore,
					ring.ProducerPosition(),
					-1,
				)
				require.NoError(t, err)
				require.False(t, ok)
				ops.assertZero(t, decoded)
			})

			t.Run("clientKeepaliveRejected", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				consumerBefore := ring.ConsumerPosition()
				producerBefore := ring.ProducerPosition()

				proxy := conductor.NewDriverProxy(ring, clientID)
				require.NoError(t, proxy.ClientKeepalive())
				advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())

				decoded, ok, err := ops.findConsumedAt(
					ring,
					clientID,
					registrationID,
					consumerBefore,
					ring.ConsumerPosition(),
					producerBefore,
					ring.ProducerPosition(),
					-1,
				)
				require.NoError(t, err)
				require.False(t, ok)
				ops.assertZero(t, decoded)
			})

			t.Run("clearedKeepaliveRejected", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				consumerBefore := ring.ConsumerPosition()
				producerBefore := ring.ProducerPosition()

				proxy := conductor.NewDriverProxy(ring, clientID)
				require.NoError(t, proxy.ClientKeepalive())
				advanceControlMinimalConsumer(t, ring, buffer, consumerBefore, ring.ProducerPosition())
				buffer.SetBytes(0, conductor.CorrelatedRingRecordLength(), 0)

				decoded, ok, err := ops.findConsumedAt(
					ring,
					clientID,
					registrationID,
					consumerBefore,
					ring.ConsumerPosition(),
					producerBefore,
					ring.ProducerPosition(),
					-1,
				)
				require.NoError(t, err)
				require.False(t, ok)
				ops.assertZero(t, decoded)
			})

			t.Run("keepaliveAndRemoveRejected", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				consumerBefore := ring.ConsumerPosition()
				producerBefore := ring.ProducerPosition()

				proxy := conductor.NewDriverProxy(ring, clientID)
				require.NoError(t, proxy.ClientKeepalive())
				ops.offer(t, ring, clientID, 42, registrationID)

				producerAfter := ring.ProducerPosition()
				advanceControlMinimalConsumerTo(t, ring, buffer, producerAfter)

				decoded, ok, err := ops.findConsumedAt(
					ring,
					clientID,
					registrationID,
					consumerBefore,
					producerAfter,
					producerBefore,
					producerAfter,
					-1,
				)
				require.NoError(t, err)
				require.False(t, ok)
				ops.assertZero(t, decoded)
			})
		})
	}
}

func TestObserveRemove_offeredWithOlderPendingMessage(t *testing.T) {
	for _, ops := range removeTestOpsVariants {
		t.Run(ops.name, func(t *testing.T) {
			ring, _ := newControlMinimalTestRingBuffer(t)

			clientID, registrationID := controlMinimalTestRemoveIDs()
			proxy := conductor.NewDriverProxy(ring, clientID)
			require.NoError(t, proxy.ClientKeepalive())

			consumerBefore := ring.ConsumerPosition()
			producerBefore := ring.ProducerPosition()
			ops.offer(t, ring, clientID, 42, registrationID)

			proven := int64(-1)
			decoded, ok, err := ops.observe(
				ring,
				clientID,
				registrationID,
				consumerBefore,
				producerBefore,
				&proven,
			)
			require.NoError(t, err)
			require.True(t, ok)
			requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
			require.Equal(t, producerBefore, proven)
		})
	}
}

func TestObserveRemove(t *testing.T) {
	tests := []struct {
		name       string
		seedProven func(consumerBefore, producerBefore int64) int64
		setup      func(
			t *testing.T,
			ops removeTestOps,
			ring *ringbuffer.ManyToOneRingBuffer,
			buffer atomic.AtomicBuffer,
			clientID, registrationID int64,
			recordLength int,
			consumerBefore, producerBefore int64,
		)
	}{
		{
			name: "offered",
			setup: func(
				t *testing.T,
				ops removeTestOps,
				ring *ringbuffer.ManyToOneRingBuffer,
				_ atomic.AtomicBuffer,
				clientID, registrationID int64,
				_ int,
				_, _ int64,
			) {
				t.Helper()
				ops.offer(t, ring, clientID, 42, registrationID)
			},
		},
		{
			name: "pending",
			setup: func(
				t *testing.T,
				ops removeTestOps,
				ring *ringbuffer.ManyToOneRingBuffer,
				_ atomic.AtomicBuffer,
				clientID, registrationID int64,
				_ int,
				_, _ int64,
			) {
				t.Helper()
				ops.offer(t, ring, clientID, 42, registrationID)
			},
			seedProven: func(consumerBefore, _ int64) int64 {
				return consumerBefore
			},
		},
		{
			name: "consumed",
			setup: func(
				t *testing.T,
				ops removeTestOps,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				clientID, registrationID int64,
				recordLength int,
				consumerBefore, _ int64,
			) {
				t.Helper()
				ops.offer(t, ring, clientID, 42, registrationID)
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)
			},
		},
		{
			name: "clearedAfterConsume",
			setup: func(
				t *testing.T,
				ops removeTestOps,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				clientID, registrationID int64,
				recordLength int,
				consumerBefore, _ int64,
			) {
				t.Helper()
				ops.offer(t, ring, clientID, 42, registrationID)
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)
				buffer.SetBytes(0, recordLength, 0)
			},
			seedProven: func(consumerBefore, _ int64) int64 {
				return consumerBefore
			},
		},
		{
			name: "clearedAfterRemoveOnlyOffer",
			setup: func(
				t *testing.T,
				ops removeTestOps,
				ring *ringbuffer.ManyToOneRingBuffer,
				buffer atomic.AtomicBuffer,
				clientID, registrationID int64,
				recordLength int,
				consumerBefore, producerBefore int64,
			) {
				t.Helper()
				require.Equal(t, consumerBefore, producerBefore)
				ops.offer(t, ring, clientID, 42, registrationID)
				advanceControlMinimalConsumerByRecordLength(
					t, ring, buffer, consumerBefore, ring.ProducerPosition(), recordLength,
				)
				buffer.SetBytes(0, recordLength, 0)
			},
		},
	}

	for _, ops := range removeTestOpsVariants {
		t.Run(ops.name, func(t *testing.T) {
			clientID, registrationID := controlMinimalTestRemoveIDs()
			recordLength := ops.recordLength()

			for _, testCase := range tests {
				t.Run(testCase.name, func(t *testing.T) {
					ring, buffer := newControlMinimalTestRingBuffer(t)
					consumerBefore := ring.ConsumerPosition()
					producerBefore := ring.ProducerPosition()
					testCase.setup(
						t,
						ops,
						ring,
						buffer,
						clientID,
						registrationID,
						recordLength,
						consumerBefore,
						producerBefore,
					)

					proven := int64(-1)
					if testCase.seedProven != nil {
						proven = testCase.seedProven(consumerBefore, producerBefore)
					}
					decoded, ok, err := ops.observe(
						ring,
						clientID,
						registrationID,
						consumerBefore,
						producerBefore,
						&proven,
					)
					require.NoError(t, err)
					require.True(t, ok)
					requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
				})
			}
		})
	}
}

func TestOfferedRecordTypeIDsBetween(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	proxy := conductor.NewDriverProxy(ring, clientID)
	consumerBefore := ring.ConsumerPosition()
	offerRemovePublicationForTest(t, ring, clientID, 1, 9001)
	offerRemoveSubscriptionForTest(t, ring, clientID, 2, 9002)
	proxy.ClientClose()

	typeIDs, err := OfferedRecordTypeIDsBetween(ring, consumerBefore, ring.ProducerPosition())
	require.NoError(t, err)
	require.Equal(
		t,
		[]int{conductor.RemovePublication, conductor.RemoveSubscription, conductor.ClientClose},
		typeIDs,
	)
}

func TestOfferedRecordTypeIDsBetween_edgeCases(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	t.Run("invalidRange", func(t *testing.T) {
		_, err := OfferedRecordTypeIDsBetween(ring, 10, 5)
		require.Error(t, err)
	})

	t.Run("equalRange", func(t *testing.T) {
		typeIDs, err := OfferedRecordTypeIDsBetween(ring, 0, 0)
		require.NoError(t, err)
		require.Nil(t, typeIDs)
	})

	t.Run("incompleteHeadTruncation", func(t *testing.T) {
		const clientID int64 = 7
		from := ring.ConsumerPosition()
		offerRemovePublicationForTest(t, ring, clientID, 1, 9001)
		buffer.PutInt32(record.LengthOffset(int(from)&(ring.Capacity()-1)), 0)

		typeIDs, err := OfferedRecordTypeIDsBetween(ring, from, ring.ProducerPosition())
		require.NoError(t, err)
		require.Empty(t, typeIDs)
	})

	t.Run("paddingOnlySpan", func(t *testing.T) {
		seedControlMinimalRingNearWrap(t, ring, buffer)
		from := ring.ProducerPosition()
		recordLength := controlMinimalTestRemovePublicationRecordLength()
		fromIndex := int(from) & (ring.Capacity() - 1)
		buffer.PutInt32(record.TypeOffset(fromIndex), int32(ringbuffer.PaddingMsgTypeID))
		buffer.PutInt32(record.LengthOffset(fromIndex), int32(controlMinimalWrapPadding))
		to := from + int64(recordLength) + controlMinimalWrapPadding

		typeIDs, err := OfferedRecordTypeIDsBetween(ring, from, to)
		require.NoError(t, err)
		require.Empty(t, typeIDs)
	})
}

func TestDecodeMatchingRemove_ignoresCorrelationID(t *testing.T) {
	const correlationID int64 = 99

	for _, ops := range removeTestOpsVariants {
		t.Run(ops.name, func(t *testing.T) {
			ring, _ := newControlMinimalTestRingBuffer(t)

			clientID, registrationID := controlMinimalTestRemoveIDs()
			ops.offer(t, ring, clientID, correlationID, registrationID)

			decoded, gotCorrelationID, ok, err := ops.findPending(ring, clientID, registrationID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, correlationID, gotCorrelationID)
			requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
		})
	}
}

func requireWrapPaddingRemoveLayout(
	t *testing.T,
	buffer atomic.AtomicBuffer,
	producerBefore int64,
	recordLength int,
	producerAfter int64,
	wantTypeID int32,
) {
	t.Helper()

	require.Equal(
		t,
		producerBefore+int64(recordLength)+controlMinimalWrapPadding,
		producerAfter,
	)
	require.Equal(
		t,
		int32(ringbuffer.PaddingMsgTypeID),
		buffer.GetInt32(record.TypeOffset(int(producerBefore))),
	)
	require.Equal(t, wantTypeID, buffer.GetInt32(record.TypeOffset(0)))
}

type removeAcrossWrapOffer struct {
	ClientID, RegistrationID                      int64
	ConsumerBefore, ProducerBefore, ProducerAfter int64
	RecordLength                                  int
}

func TestRemove_driverProxyWireLayoutParity(t *testing.T) {
	const correlationID int64 = 42

	for _, ops := range removeTestOpsVariants {
		t.Run(ops.name, func(t *testing.T) {
			ring, buffer := newControlMinimalTestRingBuffer(t)

			clientID, registrationID := controlMinimalTestRemoveIDs()
			ops.offer(t, ring, clientID, correlationID, registrationID)

			payloadOffset := ringbuffer.EncodedMsgOffset(0)
			wantBody := ops.wireBody(clientID, correlationID, registrationID)
			require.Equal(t, wantBody, buffer.Bytes()[payloadOffset:payloadOffset+len(wantBody)])

			decoded, gotCorrelationID, ok, err := ops.findPending(ring, clientID, registrationID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, correlationID, gotCorrelationID)
			requireRemoveMessageIdentity(t, clientID, registrationID, decoded)
		})
	}
}

func TestRemove_wrapPadding(t *testing.T) {
	for _, ops := range removeTestOpsVariants {
		t.Run(ops.name, func(t *testing.T) {
			t.Run("findOffered", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				wrap := ops.offerAcrossWrap(t, ring, buffer)

				var matchedPosition int64
				decoded, ok, err := ops.findOfferedAtWrap(
					ring,
					wrap.ClientID,
					wrap.RegistrationID,
					wrap.ProducerBefore,
					&matchedPosition,
				)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, wrap.ClientID, wrap.RegistrationID, decoded)
				require.Equal(t, wrap.ProducerBefore+controlMinimalWrapPadding, matchedPosition)
			})

			t.Run("findPending", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				wrap := ops.offerAcrossWrap(t, ring, buffer)

				decoded, _, ok, err := ops.findPending(ring, wrap.ClientID, wrap.RegistrationID)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, wrap.ClientID, wrap.RegistrationID, decoded)
			})

			t.Run("findConsumed", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				wrap := ops.offerAcrossWrap(t, ring, buffer)
				advanceControlMinimalConsumerTo(t, ring, buffer, wrap.ProducerAfter)

				decoded, ok, err := ops.findConsumedAt(
					ring,
					wrap.ClientID,
					wrap.RegistrationID,
					wrap.ConsumerBefore,
					wrap.ProducerAfter,
					wrap.ProducerBefore,
					wrap.ProducerAfter,
					-1,
				)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, wrap.ClientID, wrap.RegistrationID, decoded)
			})

			t.Run("findConsumed_clearedAfterConsume", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				wrap := ops.offerAcrossWrap(t, ring, buffer)
				advanceControlMinimalConsumerTo(t, ring, buffer, wrap.ProducerAfter)
				buffer.SetBytes(0, wrap.RecordLength, 0)

				decoded, ok, err := ops.findConsumedAt(
					ring,
					wrap.ClientID,
					wrap.RegistrationID,
					wrap.ConsumerBefore,
					wrap.ProducerAfter,
					wrap.ProducerBefore,
					wrap.ProducerAfter,
					wrap.ConsumerBefore,
				)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, wrap.ClientID, wrap.RegistrationID, decoded)
			})

			t.Run("observe", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				wrap := ops.offerAcrossWrap(t, ring, buffer)

				proven := int64(-1)
				decoded, ok, err := ops.observe(
					ring,
					wrap.ClientID,
					wrap.RegistrationID,
					wrap.ConsumerBefore,
					wrap.ProducerBefore,
					&proven,
				)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, wrap.ClientID, wrap.RegistrationID, decoded)
				require.Equal(t, wrap.ProducerBefore+controlMinimalWrapPadding, proven)
			})

			t.Run("observe_clearedAfterConsume", func(t *testing.T) {
				ring, buffer := newControlMinimalTestRingBuffer(t)
				wrap := ops.offerAcrossWrap(t, ring, buffer)

				proven := int64(-1)
				decoded, ok, err := ops.observe(
					ring,
					wrap.ClientID,
					wrap.RegistrationID,
					wrap.ConsumerBefore,
					wrap.ProducerBefore,
					&proven,
				)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, wrap.ClientID, wrap.RegistrationID, decoded)
				require.Equal(t, wrap.ProducerBefore+controlMinimalWrapPadding, proven)

				advanceControlMinimalConsumerTo(t, ring, buffer, wrap.ProducerAfter)
				buffer.SetBytes(0, wrap.RecordLength, 0)

				proven = wrap.ConsumerBefore
				decoded, ok, err = ops.observe(
					ring,
					wrap.ClientID,
					wrap.RegistrationID,
					wrap.ConsumerBefore,
					wrap.ProducerBefore,
					&proven,
				)
				require.NoError(t, err)
				require.True(t, ok)
				requireRemoveMessageIdentity(t, wrap.ClientID, wrap.RegistrationID, decoded)
			})
		})
	}
}

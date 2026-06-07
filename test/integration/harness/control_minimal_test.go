package harness

import (
	"testing"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
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
			buffer aeronatomic.AtomicBuffer,
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
				_ aeronatomic.AtomicBuffer,
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
				buffer aeronatomic.AtomicBuffer,
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
				buffer aeronatomic.AtomicBuffer,
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
				buffer aeronatomic.AtomicBuffer,
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
			want:          ringbuffer.HeaderLength,
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
			buffer aeronatomic.AtomicBuffer,
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
				_ aeronatomic.AtomicBuffer,
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
				_ aeronatomic.AtomicBuffer,
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
				buffer aeronatomic.AtomicBuffer,
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
				buffer aeronatomic.AtomicBuffer,
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
				buffer aeronatomic.AtomicBuffer,
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
	buffer aeronatomic.AtomicBuffer,
) {
	t.Helper()

	position := int64(controlMinimalTestDataCapacity - controlMinimalWrapPadding)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(controlMinimalTestDataCapacity), position)
	buffer.PutInt64Release(ringbuffer.HeadIndex(controlMinimalTestDataCapacity), position)
	buffer.PutInt64Release(ringbuffer.TailIndex(controlMinimalTestDataCapacity), position)
}

func requireWrapPaddingAddPublicationLayout(
	t *testing.T,
	buffer aeronatomic.AtomicBuffer,
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
	buffer aeronatomic.AtomicBuffer,
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
	buffer aeronatomic.AtomicBuffer,
	position int64,
) {
	t.Helper()

	// Harness consumed-path tests advance head by producer delta directly.
	// Real ring.Read() advances padding by aligned length (24 for 22-byte padding).
	buffer.PutInt64Release(ringbuffer.HeadIndex(ring.Capacity()), position)
}

func clearRecordAtIndex0(buffer aeronatomic.AtomicBuffer, recordLength int) {
	buffer.SetBytes(0, recordLength, 0)
}

func advanceControlMinimalConsumerByRecordLength(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer aeronatomic.AtomicBuffer,
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
	buffer aeronatomic.AtomicBuffer,
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
) (*ringbuffer.ManyToOneRingBuffer, aeronatomic.AtomicBuffer) {
	t.Helper()

	buf := make([]byte, ringbuffer.TotalBufferLength(controlMinimalTestDataCapacity))
	buffer := aeronatomic.Wrap(buf)
	ring, err := ringbuffer.NewManyToOneRingBuffer(buffer)
	require.NoError(t, err)
	return ring, buffer
}

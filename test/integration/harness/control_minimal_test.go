package harness

import (
	"testing"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const controlMinimalTestDataCapacity = 4096

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

func advanceControlMinimalConsumer(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer aeronatomic.AtomicBuffer,
	consumerBefore, producerAfter int64,
) {
	t.Helper()

	require.Equal(
		t,
		producerAfter,
		consumerBefore+int64(conductor.CorrelatedRingRecordLength()),
	)
	buffer.PutInt64Release(
		ringbuffer.HeadIndex(ring.Capacity()),
		producerAfter,
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

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const controlMinimalTestDataCapacity = 4096

func TestOfferClientKeepalive_encodeGolden(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	err := offerClientKeepalive(ring, 1)
	require.NoError(t, err)

	payloadOffset := ringbuffer.EncodedMsgOffset(0)
	bufferAtomic := aeronatomic.Wrap(buffer.Bytes())
	require.Equal(
		t,
		conductor.CorrelatedMessageBytes(1, -1),
		bufferAtomic.Bytes()[payloadOffset:payloadOffset+conductor.CorrelatedMessageLength],
	)
	got, err := conductor.DecodeCorrelated(bufferAtomic, payloadOffset)
	require.NoError(t, err)
	require.Equal(t, conductor.CorrelatedMessage{ClientID: 1, CorrelationID: -1}, got)
}

func TestOfferGetNextSessionID_encodeGolden(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	err := offerGetNextSessionID(ring, 1, 42, 100)
	require.NoError(t, err)

	payloadOffset := ringbuffer.EncodedMsgOffset(0)
	bufferAtomic := aeronatomic.Wrap(buffer.Bytes())
	correlated, err := conductor.DecodeCorrelated(bufferAtomic, payloadOffset)
	require.NoError(t, err)
	require.Equal(t, conductor.CorrelatedMessage{ClientID: 1, CorrelationID: 42}, correlated)
	require.Equal(
		t,
		int32(100),
		bufferAtomic.GetInt32(payloadOffset+conductor.CorrelatedMessageLength),
	)
}

func TestOfferCorrelatedCommand_bodyTooShort(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	err := offerCorrelatedCommand(
		ring,
		conductor.ClientKeepalive,
		conductor.CorrelatedMessageLength-1,
		1,
		-1,
		nil,
	)
	require.ErrorIs(t, err, errCorrelatedBodyTooShort)
}

func TestOfferControlMinimal_insufficientCapacity(t *testing.T) {
	tests := []struct {
		name    string
		offer   func(*ringbuffer.ManyToOneRingBuffer) error
		wantErr error
	}{
		{
			name: "client keepalive",
			offer: func(ring *ringbuffer.ManyToOneRingBuffer) error {
				return offerClientKeepalive(ring, 1)
			},
			wantErr: conductor.ErrRingInsufficientCapacity,
		},
		{
			name: "get next session id",
			offer: func(ring *ringbuffer.ManyToOneRingBuffer) error {
				return offerGetNextSessionID(ring, 1, 42, 100)
			},
			wantErr: errRingInsufficientCapacity,
		},
	}
	ring, buffer := newControlMinimalTestRingBuffer(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seedFullRingBuffer(t, buffer)

			err := tc.offer(ring)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func seedFullRingBuffer(t *testing.T, buffer aeronatomic.AtomicBuffer) {
	t.Helper()

	head := int64(0)
	tail := int64(controlMinimalTestDataCapacity)
	buffer.PutInt64Release(ringbuffer.HeadIndex(controlMinimalTestDataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(controlMinimalTestDataCapacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(controlMinimalTestDataCapacity), head)
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

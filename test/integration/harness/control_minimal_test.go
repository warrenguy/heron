package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const controlMinimalTestDataCapacity = 4096

var wantCorrelatedGolden = []byte{
	0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}

func TestOfferClientKeepalive_encodeGolden(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	err := offerClientKeepalive(ring, 1, -1)
	require.NoError(t, err)

	payloadOffset := ringbuffer.EncodedMsgOffset(0)
	got := buffer.Bytes()[payloadOffset : payloadOffset+correlatedMessageLength]
	require.Equal(t, wantCorrelatedGolden, got)
}

func TestOfferGetNextSessionID_encodeGolden(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	err := offerGetNextSessionID(ring, 1, 42, 100)
	require.NoError(t, err)

	payloadOffset := ringbuffer.EncodedMsgOffset(0)
	got := buffer.Bytes()[payloadOffset : payloadOffset+getNextAvailableSessionIDBodyLength]

	want := []byte{
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x64, 0x00, 0x00, 0x00,
	}
	require.Equal(t, want, got)
}

func TestDecodeNextSessionIDEvent_golden(t *testing.T) {
	buf := []byte{
		0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x39, 0x30, 0x00, 0x00,
	}
	buffer := aeronatomic.Wrap(buf)

	event, err := decodeNextSessionIDEvent(buffer, 0)
	require.NoError(t, err)
	require.Equal(t, int64(42), event.CorrelationID)
	require.Equal(t, int32(12345), event.NextSessionID)
}

func TestDecodeNextSessionIDEvent_outOfBounds(t *testing.T) {
	buf := make([]byte, 8)
	buffer := aeronatomic.Wrap(buf)

	_, err := decodeNextSessionIDEvent(buffer, 0)
	require.EqualError(
		t,
		err,
		"harness: next session id payload out of bounds: offset=0 capacity=8",
	)
}

func TestOfferControlMinimal_insufficientCapacity(t *testing.T) {
	tests := []struct {
		name  string
		offer func(*ringbuffer.ManyToOneRingBuffer) error
	}{
		{
			name: "client keepalive",
			offer: func(ring *ringbuffer.ManyToOneRingBuffer) error {
				return offerClientKeepalive(ring, 1, -1)
			},
		},
		{
			name: "get next session id",
			offer: func(ring *ringbuffer.ManyToOneRingBuffer) error {
				return offerGetNextSessionID(ring, 1, 42, 100)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ring, buffer := newControlMinimalTestRingBuffer(t)
			seedFullRingBuffer(t, buffer)

			err := tc.offer(ring)
			require.ErrorIs(t, err, errRingInsufficientCapacity)
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

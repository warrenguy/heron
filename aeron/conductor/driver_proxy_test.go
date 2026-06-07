package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const driverProxyTestDataCapacity = 4096

func TestDriverProxy_TimeOfLastDriverKeepaliveMs(t *testing.T) {
	ring, _ := newDriverProxyTestRing(t)
	proxy := conductor.NewDriverProxy(ring, 42)

	require.Zero(t, proxy.TimeOfLastDriverKeepaliveMs())

	want := int64(1_700_000_000_123)
	ring.SetConsumerHeartbeatTime(want)
	require.Equal(t, want, proxy.TimeOfLastDriverKeepaliveMs())
}

func TestDriverProxy_offerCorrelated_encodeGolden(t *testing.T) {
	tests := []struct {
		name       string
		clientID   int64
		wantTypeID int
		offer      func(*conductor.DriverProxy) error
	}{
		{
			name:       "client keepalive",
			clientID:   1,
			wantTypeID: conductor.ClientKeepalive,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.ClientKeepalive()
			},
		},
		{
			name:       "client close",
			clientID:   7,
			wantTypeID: conductor.ClientClose,
			offer: func(proxy *conductor.DriverProxy) error {
				proxy.ClientClose()
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ring, buffer := newDriverProxyTestRing(t)
			proxy := conductor.NewDriverProxy(ring, test.clientID)

			err := test.offer(proxy)
			require.NoError(t, err)

			payloadOffset := ringbuffer.EncodedMsgOffset(0)
			bufferAtomic := aeronatomic.Wrap(buffer.Bytes())
			require.Equal(
				t,
				conductor.CorrelatedMessageBytes(test.clientID, conductor.NullCorrelationID),
				bufferAtomic.Bytes()[payloadOffset:payloadOffset+conductor.CorrelatedMessageLength],
			)
			decoded, err := conductor.DecodeCorrelated(bufferAtomic, payloadOffset)
			require.NoError(t, err)
			require.Equal(t, conductor.CorrelatedMessage{
				ClientID:      test.clientID,
				CorrelationID: conductor.NullCorrelationID,
			}, decoded)

			var gotTypeID int
			readCount := ring.Read(func(msgTypeID int, buffer aeronatomic.AtomicBuffer, index, length int) {
				gotTypeID = msgTypeID
				got, decodeErr := conductor.DecodeCorrelated(buffer, index)
				require.NoError(t, decodeErr)
				require.Equal(t, decoded, got)
				require.Equal(t, conductor.CorrelatedMessageLength, length)
			})
			require.Equal(t, 1, readCount)
			require.Equal(t, test.wantTypeID, gotTypeID)
		})
	}
}

func TestDriverProxy_ClientKeepalive_insufficientCapacity(t *testing.T) {
	ring, buffer := newDriverProxyTestRing(t)
	seedFullDriverProxyRing(t, buffer)

	proxy := conductor.NewDriverProxy(ring, 1)
	err := proxy.ClientKeepalive()
	require.ErrorIs(t, err, conductor.ErrRingInsufficientCapacity)
}

func TestDriverProxy_offerCorrelated_invalidTypeID(t *testing.T) {
	tests := []struct {
		name  string
		offer func(*conductor.DriverProxy) error
	}{
		{
			name: "non-silent",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.OfferCorrelatedForTest(-1)
			},
		},
		{
			name: "silent",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.OfferCorrelatedSilentForTest(-1)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ring, _ := newDriverProxyTestRing(t)
			proxy := conductor.NewDriverProxy(ring, 1)

			err := tc.offer(proxy)
			require.ErrorIs(t, err, record.ErrInvalidMessageTypeID)
			require.Zero(t, ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
				t.Fatal("invalid type should not write")
			}))
		})
	}
}

func TestDriverProxy_offerCorrelated_encodeFailureAborts(t *testing.T) {
	tests := []struct {
		name      string
		msgTypeID int
	}{
		{name: "non-silent", msgTypeID: conductor.ClientKeepalive},
		{name: "silent", msgTypeID: conductor.ClientClose},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ring, buffer := newDriverProxyTestRing(t)
			proxy := conductor.NewDriverProxy(ring, 1)

			encodedOffset, err := ring.TryClaim(tc.msgTypeID, conductor.CorrelatedMessageLength)
			require.NoError(t, err)
			require.NotEqual(t, ringbuffer.InsufficientCapacity, encodedOffset)

			err = proxy.EncodeCommitCorrelatedAtForTest(encodedOffset, buffer.Capacity())
			require.ErrorIs(t, err, conductor.ErrCorrelatedMessageOutOfBounds)
			require.Zero(t, ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
				t.Fatal("aborted claim should not commit")
			}))
		})
	}
}

func TestDriverProxy_ClientClose_insufficientCapacity(t *testing.T) {
	ring, buffer := newDriverProxyTestRing(t)
	seedFullDriverProxyRing(t, buffer)

	proxy := conductor.NewDriverProxy(ring, 1)
	proxy.ClientClose()

	require.Zero(t, ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
		t.Fatal("close should not write when ring is full")
	}))
}

func newDriverProxyTestRing(
	tb testing.TB,
) (*ringbuffer.ManyToOneRingBuffer, aeronatomic.AtomicBuffer) {
	tb.Helper()

	buf := make([]byte, ringbuffer.TotalBufferLength(driverProxyTestDataCapacity))
	buffer := aeronatomic.Wrap(buf)
	ring, err := ringbuffer.NewManyToOneRingBuffer(buffer)
	require.NoError(tb, err)
	return ring, buffer
}

func seedFullDriverProxyRing(tb testing.TB, buffer aeronatomic.AtomicBuffer) {
	tb.Helper()

	head := int64(0)
	tail := int64(driverProxyTestDataCapacity)
	buffer.PutInt64Release(ringbuffer.HeadIndex(driverProxyTestDataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(driverProxyTestDataCapacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(driverProxyTestDataCapacity), head)
}

package conductor_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/agrona/record"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

const (
	driverProxyTestDataCapacity = 4096
	driverProxyTestChannel      = "aeron:ipc"
)

func TestDriverProxy_LastDriverKeepaliveMs(t *testing.T) {
	ring, _ := newDriverProxyTestRing(t)
	proxy := conductor.NewDriverProxy(ring, 42)

	require.Zero(t, proxy.LastDriverKeepaliveMs())

	want := int64(1_700_000_000_123)
	ring.SetConsumerHeartbeatTime(want)
	require.Equal(t, want, proxy.LastDriverKeepaliveMs())
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
			bufferAtomic := atomic.Wrap(buffer.Bytes())
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
			readCount := ring.Read(func(msgTypeID int, buffer atomic.AtomicBuffer, index, length int) {
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

func TestDriverProxy_NextAvailableSessionID_encodeGolden(t *testing.T) {
	const (
		clientID      int64 = 1
		correlationID int64 = 42
		streamID      int32 = 100
	)

	ring, buffer := newDriverProxyTestRing(t)
	proxy := conductor.NewDriverProxy(ring, clientID)

	err := proxy.NextAvailableSessionID(correlationID, streamID)
	require.NoError(t, err)

	payloadOffset := ringbuffer.EncodedMsgOffset(0)
	bufferAtomic := atomic.Wrap(buffer.Bytes())
	require.Equal(
		t,
		conductor.CorrelatedMessageBytes(clientID, correlationID),
		bufferAtomic.Bytes()[payloadOffset:payloadOffset+conductor.CorrelatedMessageLength],
	)
	require.Equal(
		t,
		streamID,
		bufferAtomic.GetInt32(payloadOffset+conductor.CorrelatedMessageLength),
	)

	decoded, err := conductor.DecodeCorrelated(bufferAtomic, payloadOffset)
	require.NoError(t, err)
	require.Equal(t, conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: correlationID,
	}, decoded)

	var gotTypeID int
	readCount := ring.Read(func(msgTypeID int, buffer atomic.AtomicBuffer, index, length int) {
		gotTypeID = msgTypeID
		got, decodeErr := conductor.DecodeCorrelated(buffer, index)
		require.NoError(t, decodeErr)
		require.Equal(t, decoded, got)
		require.Equal(t, streamID, buffer.GetInt32(index+conductor.CorrelatedMessageLength))
		require.Equal(t, conductor.GetNextAvailableSessionIDBodyLength, length)
	})
	require.Equal(t, 1, readCount)
	require.Equal(t, conductor.GetNextAvailableSessionID, gotTypeID)
}

func TestDriverProxy_NextAvailableSessionID_insufficientCapacity(t *testing.T) {
	ring, buffer := newDriverProxyTestRing(t)
	seedFullDriverProxyRing(t, buffer)

	proxy := conductor.NewDriverProxy(ring, 1)
	err := proxy.NextAvailableSessionID(42, 100)
	require.ErrorIs(t, err, conductor.ErrRingInsufficientCapacity)
}

func TestDriverProxy_ClientKeepalive_insufficientCapacity(t *testing.T) {
	ring, buffer := newDriverProxyTestRing(t)
	seedFullDriverProxyRing(t, buffer)

	proxy := conductor.NewDriverProxy(ring, 1)
	err := proxy.ClientKeepalive()
	require.ErrorIs(t, err, conductor.ErrRingInsufficientCapacity)
}

func TestDriverProxy_offerCorrelatedBody_bodyTooShort(t *testing.T) {
	ring, _ := newDriverProxyTestRing(t)
	proxy := conductor.NewDriverProxy(ring, 1)

	err := proxy.OfferCorrelatedBodyForTest(
		conductor.ClientKeepalive,
		conductor.CorrelatedMessageLength-1,
		conductor.NullCorrelationID,
		false,
		nil,
	)
	require.ErrorIs(t, err, conductor.ErrCorrelatedBodyTooShort)
	require.Zero(t, ring.Read(func(int, atomic.AtomicBuffer, int, int) {
		t.Fatal("short body should not claim")
	}))
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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ring, _ := newDriverProxyTestRing(t)
			proxy := conductor.NewDriverProxy(ring, 1)

			err := test.offer(proxy)
			require.ErrorIs(t, err, record.ErrInvalidMessageTypeID)
			require.Zero(t, ring.Read(func(int, atomic.AtomicBuffer, int, int) {
				t.Fatal("invalid type should not write")
			}))
		})
	}
}

func TestDriverProxy_offerCorrelated_encodeFailureAborts(t *testing.T) {
	input := driverProxyEncodeFailureInput{
		ClientID:      1,
		CorrelationID: conductor.NullCorrelationID,
	}
	t.Run("offer body", func(t *testing.T) {
		runDriverProxyEncodeFailureAbort(t, driverProxyEncodeFailureCorrelated, input)
	})
	t.Run("offer correlated body client keepalive", func(t *testing.T) {
		runDriverProxyOfferCorrelatedEncodeFailureAbort(t, input)
	})
}

func requireDriverProxyPublicationMessage(
	t *testing.T,
	buffer atomic.AtomicBuffer,
	index, length int,
	want conductor.PublicationMessage,
) {
	t.Helper()

	got, err := conductor.DecodePublicationMessage(buffer, index, length)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestDriverProxy_offerBody_encodeGolden(t *testing.T) {
	const (
		clientID       int64 = 1
		correlationID  int64 = 42
		streamID       int32 = 1001
		registrationID int64 = 9001
	)

	publicationWant := conductor.PublicationMessage{
		ClientID:      clientID,
		CorrelationID: correlationID,
		StreamID:      streamID,
		Channel:       driverProxyTestChannel,
	}
	publicationWantBody := conductor.PublicationMessageBytes(
		clientID,
		correlationID,
		streamID,
		driverProxyTestChannel,
	)
	publicationBodyLength := conductor.PublicationMessageBodyLength(driverProxyTestChannel)
	requirePublication := func(t *testing.T, buffer atomic.AtomicBuffer, index, length int) {
		requireDriverProxyPublicationMessage(t, buffer, index, length, publicationWant)
	}

	tests := []struct {
		name       string
		wantTypeID int
		offer      func(*conductor.DriverProxy) error
		wantBody   []byte
		bodyLength int
		read       func(t *testing.T, buffer atomic.AtomicBuffer, index, length int)
	}{
		{
			name:       "add publication",
			wantTypeID: conductor.AddPublication,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddPublication(correlationID, streamID, driverProxyTestChannel)
			},
			wantBody:   publicationWantBody,
			bodyLength: publicationBodyLength,
			read:       requirePublication,
		},
		{
			name:       "add exclusive publication",
			wantTypeID: conductor.AddExclusivePublication,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddExclusivePublication(correlationID, streamID, driverProxyTestChannel)
			},
			wantBody:   publicationWantBody,
			bodyLength: publicationBodyLength,
			read:       requirePublication,
		},
		{
			name:       "add subscription",
			wantTypeID: conductor.AddSubscription,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddSubscription(correlationID, streamID, driverProxyTestChannel)
			},
			wantBody: conductor.SubscriptionMessageBytes(
				clientID,
				correlationID,
				conductor.NullRegistrationCorrelationID,
				streamID,
				driverProxyTestChannel,
			),
			bodyLength: conductor.SubscriptionMessageBodyLength(driverProxyTestChannel),
			read: func(t *testing.T, buffer atomic.AtomicBuffer, index, length int) {
				got, err := conductor.DecodeSubscriptionMessage(buffer, index, length)
				require.NoError(t, err)
				require.Equal(t, conductor.SubscriptionMessage{
					ClientID:                  clientID,
					CorrelationID:             correlationID,
					RegistrationCorrelationID: conductor.NullRegistrationCorrelationID,
					StreamID:                  streamID,
					Channel:                   driverProxyTestChannel,
				}, got)
			},
		},
		{
			name:       "remove publication",
			wantTypeID: conductor.RemovePublication,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemovePublication(correlationID, registrationID, true)
			},
			wantBody: conductor.RemovePublicationMessageBytes(
				clientID,
				correlationID,
				registrationID,
				true,
			),
			bodyLength: conductor.RemovePublicationBodyLength,
			read: func(t *testing.T, buffer atomic.AtomicBuffer, index, length int) {
				got, err := conductor.DecodeRemovePublicationMessage(
					buffer,
					index,
					conductor.RemovePublicationBodyLength,
				)
				require.NoError(t, err)
				require.Equal(t, conductor.RemovePublicationMessage{
					RemoveMessage: conductor.RemoveMessage{
						ClientID:       clientID,
						CorrelationID:  correlationID,
						RegistrationID: registrationID,
					},
					Revoke: true,
				}, got)
			},
		},
		{
			name:       "remove publication without revoke",
			wantTypeID: conductor.RemovePublication,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemovePublication(correlationID, registrationID, false)
			},
			wantBody: conductor.RemovePublicationMessageBytes(
				clientID,
				correlationID,
				registrationID,
				false,
			),
			bodyLength: conductor.RemovePublicationBodyLength,
			read: func(t *testing.T, buffer atomic.AtomicBuffer, index, length int) {
				got, err := conductor.DecodeRemovePublicationMessage(
					buffer,
					index,
					conductor.RemovePublicationBodyLength,
				)
				require.NoError(t, err)
				require.Equal(t, conductor.RemovePublicationMessage{
					RemoveMessage: conductor.RemoveMessage{
						ClientID:       clientID,
						CorrelationID:  correlationID,
						RegistrationID: registrationID,
					},
					Revoke: false,
				}, got)
			},
		},
		{
			name:       "remove subscription",
			wantTypeID: conductor.RemoveSubscription,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemoveSubscription(correlationID, registrationID)
			},
			wantBody: conductor.RemoveMessageBytes(
				clientID,
				correlationID,
				registrationID,
			),
			bodyLength: conductor.RemoveMessageBodyLength,
			read: func(t *testing.T, buffer atomic.AtomicBuffer, index, length int) {
				got, err := conductor.DecodeRemoveMessage(buffer, index, length)
				require.NoError(t, err)
				require.Equal(t, conductor.RemoveMessage{
					ClientID:       clientID,
					CorrelationID:  correlationID,
					RegistrationID: registrationID,
				}, got)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ring, buffer := newDriverProxyTestRing(t)
			proxy := conductor.NewDriverProxy(ring, clientID)

			err := test.offer(proxy)
			require.NoError(t, err)

			payloadOffset := ringbuffer.EncodedMsgOffset(0)
			bufferAtomic := atomic.Wrap(buffer.Bytes())
			require.Equal(
				t,
				test.wantBody,
				bufferAtomic.Bytes()[payloadOffset:payloadOffset+len(test.wantBody)],
			)

			var gotTypeID int
			readCount := ring.Read(func(msgTypeID int, buffer atomic.AtomicBuffer, index, length int) {
				gotTypeID = msgTypeID
				test.read(t, buffer, index, length)
				require.Equal(t, test.bodyLength, length)
			})
			require.Equal(t, 1, readCount)
			require.Equal(t, test.wantTypeID, gotTypeID)
		})
	}
}

func TestDriverProxy_offerBody_insufficientCapacity(t *testing.T) {
	tests := []struct {
		name  string
		offer func(*conductor.DriverProxy) error
	}{
		{
			name: "add publication",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddPublication(42, 1001, driverProxyTestChannel)
			},
		},
		{
			name: "add exclusive publication",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddExclusivePublication(42, 1001, driverProxyTestChannel)
			},
		},
		{
			name: "add subscription",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddSubscription(42, 1001, driverProxyTestChannel)
			},
		},
		{
			name: "remove publication",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemovePublication(42, 9001, false)
			},
		},
		{
			name: "remove subscription",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemoveSubscription(42, 9001)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ring, buffer := newDriverProxyTestRing(t)
			seedFullDriverProxyRing(t, buffer)

			proxy := conductor.NewDriverProxy(ring, 1)
			err := test.offer(proxy)
			require.ErrorIs(t, err, conductor.ErrRingInsufficientCapacity)
		})
	}
}

func TestDriverProxy_addChannelCommands_rejectNonASCII(t *testing.T) {
	const (
		correlationID int64 = 42
		streamID      int32 = 1001
	)
	channels := []struct {
		name    string
		channel string
	}{
		{name: "high byte", channel: "aeron:\x80"},
		{name: "utf8 multibyte", channel: "\u00fc"},
	}
	tests := []struct {
		name  string
		offer func(*conductor.DriverProxy, string) error
	}{
		{
			name: "add publication",
			offer: func(proxy *conductor.DriverProxy, channel string) error {
				return proxy.AddPublication(correlationID, streamID, channel)
			},
		},
		{
			name: "add exclusive publication",
			offer: func(proxy *conductor.DriverProxy, channel string) error {
				return proxy.AddExclusivePublication(correlationID, streamID, channel)
			},
		},
		{
			name: "add subscription",
			offer: func(proxy *conductor.DriverProxy, channel string) error {
				return proxy.AddSubscription(correlationID, streamID, channel)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, channelTest := range channels {
				t.Run(channelTest.name, func(t *testing.T) {
					ring, _ := newDriverProxyTestRing(t)
					proxy := conductor.NewDriverProxy(ring, 1)
					err := test.offer(proxy, channelTest.channel)
					require.ErrorIs(t, err, atomic.ErrStringASCIINonASCII)
					requireZeroDriverProxyCommittedRecords(t, ring)
				})
			}
		})
	}
}

func TestDriverProxy_offerBody_encodeFailureAborts(t *testing.T) {
	const (
		clientID       int64 = 1
		correlationID  int64 = 42
		streamID       int32 = 1001
		registrationID int64 = 9001
	)
	input := driverProxyEncodeFailureInput{
		ClientID:                  clientID,
		CorrelationID:             correlationID,
		StreamID:                  streamID,
		RegistrationID:            registrationID,
		Channel:                   driverProxyTestChannel,
		RegistrationCorrelationID: conductor.NullRegistrationCorrelationID,
	}
	tests := []struct {
		name string
		kind driverProxyEncodeFailureKind
	}{
		{name: "add publication", kind: driverProxyEncodeFailurePublication},
		{name: "add subscription", kind: driverProxyEncodeFailureSubscription},
		{name: "remove publication", kind: driverProxyEncodeFailureRemovePublication},
		{name: "remove subscription", kind: driverProxyEncodeFailureRemoveSubscription},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runDriverProxyEncodeFailureAbort(t, test.kind, input)
		})
	}
	t.Run("add publication separate buffer wrong offset", func(t *testing.T) {
		runDriverProxyOfferBodyEncodeFailureAbortSeparateBuffer(
			t,
			driverProxyEncodeFailurePublication,
			input,
		)
	})
}

func runDriverProxyOfferBodyEncodeFailureAbortSeparateBuffer(
	tb testing.TB,
	kind driverProxyEncodeFailureKind,
	input driverProxyEncodeFailureInput,
) {
	tb.Helper()

	encodeCase := driverProxyEncodeFailureCaseForKind(kind, input)
	ring, buffer := newDriverProxyTestRing(tb)
	proxy := conductor.NewDriverProxy(ring, input.ClientID)

	err := proxy.OfferBodyForTest(
		encodeCase.MsgTypeID,
		encodeCase.BodyLength,
		false,
		func(_ int) error {
			return encodeCase.Encode(buffer, driverProxyEncodeAtWrongOffset(buffer))
		},
	)
	require.ErrorIs(tb, err, encodeCase.WantErr)
	requireZeroDriverProxyCommittedRecords(tb, ring)
}

func TestDriverProxy_offerBody_invalidTypeID(t *testing.T) {
	ring, _ := newDriverProxyTestRing(t)
	proxy := conductor.NewDriverProxy(ring, 1)

	err := proxy.OfferBodyForTest(
		-1,
		conductor.PublicationMessageBodyLength(driverProxyTestChannel),
		false,
		func(_ int) error { return nil },
	)
	require.ErrorIs(t, err, record.ErrInvalidMessageTypeID)
	require.Zero(t, ring.Read(func(int, atomic.AtomicBuffer, int, int) {
		t.Fatal("invalid type should not write")
	}))
}

func TestDriverProxy_ClientClose_insufficientCapacity(t *testing.T) {
	ring, buffer := newDriverProxyTestRing(t)
	seedFullDriverProxyRing(t, buffer)

	proxy := conductor.NewDriverProxy(ring, 1)
	proxy.ClientClose()

	require.Zero(t, ring.Read(func(int, atomic.AtomicBuffer, int, int) {
		t.Fatal("close should not write when ring is full")
	}))
}

type driverProxyEncodeFailureKind int

const (
	driverProxyEncodeFailureCorrelated driverProxyEncodeFailureKind = iota
	driverProxyEncodeFailurePublication
	driverProxyEncodeFailureSubscription
	driverProxyEncodeFailureRemovePublication
	driverProxyEncodeFailureRemoveSubscription
)

type driverProxyEncodeFailureInput struct {
	ClientID                  int64
	CorrelationID             int64
	StreamID                  int32
	RegistrationID            int64
	Revoke                    bool
	Channel                   string
	RegistrationCorrelationID int64
}

type driverProxyEncodeFailureCase struct {
	MsgTypeID       int
	BodyLength      int
	Encode          func(buf atomic.AtomicBuffer, encodedOffset int) error
	WantErr         error
	OutOfBoundsKind string
}

func driverProxyEncodeAtClaimedOffset(_ atomic.AtomicBuffer, encodedOffset int) int {
	return encodedOffset
}

func driverProxyEncodeAtWrongOffset(buf atomic.AtomicBuffer) int {
	return buf.Capacity()
}

func driverProxyEncodeFailureWriteOffset(
	buf atomic.AtomicBuffer,
	encodedOffset int,
	wrongOffset bool,
) int {
	if wrongOffset {
		return driverProxyEncodeAtWrongOffset(buf)
	}
	return driverProxyEncodeAtClaimedOffset(buf, encodedOffset)
}

func drawDriverProxyEncodeFailureKind(rt *rapid.T, name string) driverProxyEncodeFailureKind {
	return driverProxyEncodeFailureKind(
		rapid.IntRange(0, int(driverProxyEncodeFailureRemoveSubscription)).Draw(rt, name),
	)
}

func driverProxyEncodeFailureOutOfBoundsKind(kind driverProxyEncodeFailureKind) string {
	switch kind {
	case driverProxyEncodeFailureCorrelated:
		return "correlated"
	case driverProxyEncodeFailurePublication:
		return "publication"
	case driverProxyEncodeFailureSubscription:
		return "subscription"
	case driverProxyEncodeFailureRemovePublication, driverProxyEncodeFailureRemoveSubscription:
		return "remove"
	default:
		panic(fmt.Sprintf("unknown driverProxyEncodeFailureKind: %d", kind))
	}
}

func driverProxyOutOfBoundsMsg(kind string, offset, capacity int) string {
	return fmt.Sprintf(
		"conductor: %s message out of bounds: offset=%d capacity=%d",
		kind,
		offset,
		capacity,
	)
}

func driverProxyEncodeFailureCaseForKind(
	kind driverProxyEncodeFailureKind,
	input driverProxyEncodeFailureInput,
) driverProxyEncodeFailureCase {
	outOfBoundsKind := driverProxyEncodeFailureOutOfBoundsKind(kind)
	switch kind {
	case driverProxyEncodeFailureCorrelated:
		return driverProxyEncodeFailureCase{
			MsgTypeID:  conductor.ClientKeepalive,
			BodyLength: conductor.CorrelatedMessageLength,
			Encode: func(buf atomic.AtomicBuffer, encodedOffset int) error {
				return conductor.EncodeCorrelated(
					buf,
					driverProxyEncodeFailureWriteOffset(buf, encodedOffset, true),
					input.ClientID,
					input.CorrelationID,
				)
			},
			WantErr:         conductor.ErrCorrelatedMessageOutOfBounds,
			OutOfBoundsKind: outOfBoundsKind,
		}
	case driverProxyEncodeFailurePublication:
		return driverProxyEncodeFailureCase{
			MsgTypeID:  conductor.AddPublication,
			BodyLength: conductor.PublicationMessageBodyLength(input.Channel),
			Encode: func(buf atomic.AtomicBuffer, encodedOffset int) error {
				return conductor.EncodePublicationMessage(
					buf,
					driverProxyEncodeFailureWriteOffset(buf, encodedOffset, true),
					input.ClientID,
					input.CorrelationID,
					input.StreamID,
					input.Channel,
				)
			},
			WantErr:         conductor.ErrPublicationMessageOutOfBounds,
			OutOfBoundsKind: outOfBoundsKind,
		}
	case driverProxyEncodeFailureSubscription:
		return driverProxyEncodeFailureCase{
			MsgTypeID:  conductor.AddSubscription,
			BodyLength: conductor.SubscriptionMessageBodyLength(input.Channel),
			Encode: func(buf atomic.AtomicBuffer, encodedOffset int) error {
				return conductor.EncodeSubscriptionMessage(
					buf,
					driverProxyEncodeFailureWriteOffset(buf, encodedOffset, true),
					input.ClientID,
					input.CorrelationID,
					input.RegistrationCorrelationID,
					input.StreamID,
					input.Channel,
				)
			},
			WantErr:         conductor.ErrSubscriptionMessageOutOfBounds,
			OutOfBoundsKind: outOfBoundsKind,
		}
	case driverProxyEncodeFailureRemovePublication:
		return driverProxyEncodeFailureCase{
			MsgTypeID:  conductor.RemovePublication,
			BodyLength: conductor.RemovePublicationBodyLength,
			Encode: func(buf atomic.AtomicBuffer, encodedOffset int) error {
				return conductor.EncodeRemovePublicationMessage(
					buf,
					driverProxyEncodeFailureWriteOffset(buf, encodedOffset, true),
					input.ClientID,
					input.CorrelationID,
					input.RegistrationID,
					input.Revoke,
				)
			},
			WantErr:         conductor.ErrRemoveMessageOutOfBounds,
			OutOfBoundsKind: outOfBoundsKind,
		}
	case driverProxyEncodeFailureRemoveSubscription:
		return driverProxyEncodeFailureCase{
			MsgTypeID:  conductor.RemoveSubscription,
			BodyLength: conductor.RemoveMessageBodyLength,
			Encode: func(buf atomic.AtomicBuffer, encodedOffset int) error {
				return conductor.EncodeRemoveMessage(
					buf,
					driverProxyEncodeFailureWriteOffset(buf, encodedOffset, true),
					input.ClientID,
					input.CorrelationID,
					input.RegistrationID,
				)
			},
			WantErr:         conductor.ErrRemoveMessageOutOfBounds,
			OutOfBoundsKind: outOfBoundsKind,
		}
	default:
		panic(fmt.Sprintf("unknown driverProxyEncodeFailureKind: %d", kind))
	}
}

func runDriverProxyEncodeFailureAbort(
	tb testing.TB,
	kind driverProxyEncodeFailureKind,
	input driverProxyEncodeFailureInput,
) {
	tb.Helper()

	runDriverProxyOfferBodyEncodeFailureAbort(
		tb,
		driverProxyEncodeFailureCaseForKind(kind, input),
		input.ClientID,
	)
}

func runDriverProxyOfferBodyEncodeFailureAbort(
	tb testing.TB,
	encodeCase driverProxyEncodeFailureCase,
	clientID int64,
) {
	tb.Helper()

	ring, _ := newDriverProxyTestRing(tb)
	proxy := conductor.NewDriverProxy(ring, clientID)

	err := proxy.OfferBodyForTest(
		encodeCase.MsgTypeID,
		encodeCase.BodyLength,
		false,
		func(encodedOffset int) error {
			return encodeCase.Encode(proxy.Ring().Buffer(), encodedOffset)
		},
	)
	require.ErrorIs(tb, err, encodeCase.WantErr)
	requireZeroDriverProxyCommittedRecords(tb, ring)
}

func runDriverProxyOfferCorrelatedEncodeFailureAbort(
	tb testing.TB,
	input driverProxyEncodeFailureInput,
) {
	tb.Helper()

	encodeCase := driverProxyEncodeFailureCaseForKind(
		driverProxyEncodeFailureCorrelated,
		input,
	)
	ring, _ := newDriverProxyTestRing(tb)
	proxy := conductor.NewDriverProxy(ring, input.ClientID)

	err := proxy.OfferCorrelatedBodyForTest(
		encodeCase.MsgTypeID,
		encodeCase.BodyLength,
		input.CorrelationID,
		false,
		func(encodedOffset int) error {
			return conductor.EncodeCorrelated(
				proxy.Ring().Buffer(),
				driverProxyEncodeFailureWriteOffset(
					proxy.Ring().Buffer(),
					encodedOffset,
					true,
				),
				input.ClientID,
				input.CorrelationID,
			)
		},
	)
	require.ErrorIs(tb, err, encodeCase.WantErr)
	requireZeroDriverProxyCommittedRecords(tb, ring)
}

func requireZeroDriverProxyCommittedRecords(
	tb testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
) {
	tb.Helper()

	require.Zero(tb, testutil.CommittedNonPaddingRecordCount(ring), "aborted claim should not commit")
}

func newDriverProxyTestRing(
	tb testing.TB,
) (*ringbuffer.ManyToOneRingBuffer, atomic.AtomicBuffer) {
	tb.Helper()

	buf := make([]byte, ringbuffer.TotalBufferLength(driverProxyTestDataCapacity))
	buffer := atomic.Wrap(buf)
	ring, err := ringbuffer.NewManyToOneRingBuffer(buffer)
	require.NoError(tb, err)
	return ring, buffer
}

func seedFullDriverProxyRing(tb testing.TB, buffer atomic.AtomicBuffer) {
	tb.Helper()

	head := int64(0)
	tail := int64(driverProxyTestDataCapacity)
	buffer.PutInt64Release(ringbuffer.HeadIndex(driverProxyTestDataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(driverProxyTestDataCapacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(driverProxyTestDataCapacity), head)
}

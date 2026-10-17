package conductor_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/record"
	"github.com/warrenguy/heron/ringbuffer"
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
			bufferAtomic := atomicbuffer.Wrap(buffer.Bytes())
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
			readCount := ring.Read(func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) {
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
	bufferAtomic := atomicbuffer.Wrap(buffer.Bytes())
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
	readCount := ring.Read(func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) {
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
	requireZeroDriverProxyCommittedRecords(t, ring)
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
			requireZeroDriverProxyCommittedRecords(t, ring)
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
	buffer atomicbuffer.AtomicBuffer,
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
	requirePublication := func(t *testing.T, buffer atomicbuffer.AtomicBuffer, index, length int) {
		requireDriverProxyPublicationMessage(t, buffer, index, length, publicationWant)
	}

	destinationWant := conductor.DestinationMessage{
		ClientID:                  clientID,
		CorrelationID:             correlationID,
		RegistrationCorrelationID: registrationID,
		Channel:                   driverProxyTestChannel,
	}
	destinationWantBody := conductor.DestinationMessageBytes(
		clientID,
		correlationID,
		registrationID,
		driverProxyTestChannel,
	)
	destinationBodyLength := conductor.DestinationMessageBodyLength(driverProxyTestChannel)
	requireDestination := func(t *testing.T, buffer atomicbuffer.AtomicBuffer, index, length int) {
		got, err := conductor.DecodeDestinationMessage(buffer, index, length)
		require.NoError(t, err)
		require.Equal(t, destinationWant, got)
	}

	removePublicationWantBody := func(revoke bool) []byte {
		return conductor.RemovePublicationMessageBytes(
			clientID,
			correlationID,
			registrationID,
			revoke,
		)
	}
	requireRemovePublication := func(revoke bool) func(
		t *testing.T,
		buffer atomicbuffer.AtomicBuffer,
		index, length int,
	) {
		return func(t *testing.T, buffer atomicbuffer.AtomicBuffer, index, length int) {
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
				Revoke: revoke,
			}, got)
		}
	}

	tests := []struct {
		name       string
		wantTypeID int
		offer      func(*conductor.DriverProxy) error
		wantBody   []byte
		bodyLength int
		read       func(t *testing.T, buffer atomicbuffer.AtomicBuffer, index, length int)
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
			read: func(t *testing.T, buffer atomicbuffer.AtomicBuffer, index, length int) {
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
			wantBody:   removePublicationWantBody(true),
			bodyLength: conductor.RemovePublicationBodyLength,
			read:       requireRemovePublication(true),
		},
		{
			name:       "remove publication without revoke",
			wantTypeID: conductor.RemovePublication,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemovePublication(correlationID, registrationID, false)
			},
			wantBody:   removePublicationWantBody(false),
			bodyLength: conductor.RemovePublicationBodyLength,
			read:       requireRemovePublication(false),
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
			read: func(t *testing.T, buffer atomicbuffer.AtomicBuffer, index, length int) {
				got, err := conductor.DecodeRemoveMessage(buffer, index, length)
				require.NoError(t, err)
				require.Equal(t, conductor.RemoveMessage{
					ClientID:       clientID,
					CorrelationID:  correlationID,
					RegistrationID: registrationID,
				}, got)
			},
		},
		{
			name:       "add counter",
			wantTypeID: conductor.AddCounter,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddCounter(
					correlationID,
					counterTestTypeID,
					counterTestKey,
					counterTestLabel,
				)
			},
			wantBody: conductor.CounterMessageBytes(
				clientID,
				correlationID,
				counterTestTypeID,
				counterTestKey,
				counterTestLabel,
			),
			bodyLength: conductor.CounterMessageBodyLength(
				len(counterTestKey),
				len(counterTestLabel),
			),
			read: func(t *testing.T, buffer atomicbuffer.AtomicBuffer, index, length int) {
				got, err := conductor.DecodeCounterMessage(buffer, index, length)
				require.NoError(t, err)
				require.Equal(t, conductor.CounterMessage{
					ClientID:      clientID,
					CorrelationID: correlationID,
					TypeID:        counterTestTypeID,
					Key:           counterTestKey,
					Label:         counterTestLabel,
				}, got)
			},
		},
		{
			name:       "remove counter",
			wantTypeID: conductor.RemoveCounter,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemoveCounter(correlationID, registrationID)
			},
			wantBody: conductor.RemoveMessageBytes(
				clientID,
				correlationID,
				registrationID,
			),
			bodyLength: conductor.RemoveMessageBodyLength,
			read: func(t *testing.T, buffer atomicbuffer.AtomicBuffer, index, length int) {
				got, err := conductor.DecodeRemoveMessage(buffer, index, length)
				require.NoError(t, err)
				require.Equal(t, conductor.RemoveMessage{
					ClientID:       clientID,
					CorrelationID:  correlationID,
					RegistrationID: registrationID,
				}, got)
			},
		},
		{
			name:       "add receive destination",
			wantTypeID: conductor.AddReceiveDestination,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddReceiveDestination(
					correlationID,
					registrationID,
					driverProxyTestChannel,
				)
			},
			wantBody:   destinationWantBody,
			bodyLength: destinationBodyLength,
			read:       requireDestination,
		},
		{
			name:       "remove receive destination",
			wantTypeID: conductor.RemoveReceiveDestination,
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemoveReceiveDestination(
					correlationID,
					registrationID,
					driverProxyTestChannel,
				)
			},
			wantBody:   destinationWantBody,
			bodyLength: destinationBodyLength,
			read:       requireDestination,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ring, buffer := newDriverProxyTestRing(t)
			proxy := conductor.NewDriverProxy(ring, clientID)

			err := test.offer(proxy)
			require.NoError(t, err)

			payloadOffset := ringbuffer.EncodedMsgOffset(0)
			bufferAtomic := atomicbuffer.Wrap(buffer.Bytes())
			require.Equal(
				t,
				test.wantBody,
				bufferAtomic.Bytes()[payloadOffset:payloadOffset+len(test.wantBody)],
			)

			var gotTypeID int
			readCount := ring.Read(func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) {
				gotTypeID = msgTypeID
				test.read(t, buffer, index, length)
				require.Equal(t, test.bodyLength, length)
			})
			require.Equal(t, 1, readCount)
			require.Equal(t, test.wantTypeID, gotTypeID)
		})
	}
}

func TestDriverProxy_offer_insufficientCapacity(t *testing.T) {
	tests := []struct {
		name  string
		offer func(*conductor.DriverProxy) error
	}{
		{
			name: "client keepalive",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.ClientKeepalive()
			},
		},
		{
			name: "next available session id",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.NextAvailableSessionID(42, 100)
			},
		},
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
		{
			name: "add counter",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddCounter(42, counterTestTypeID, counterTestKey, counterTestLabel)
			},
		},
		{
			name: "remove counter",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemoveCounter(42, 9001)
			},
		},
		{
			name: "add receive destination",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.AddReceiveDestination(42, 9001, driverProxyTestChannel)
			},
		},
		{
			name: "remove receive destination",
			offer: func(proxy *conductor.DriverProxy) error {
				return proxy.RemoveReceiveDestination(42, 9001, driverProxyTestChannel)
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
		{
			name: "add receive destination",
			offer: func(proxy *conductor.DriverProxy, channel string) error {
				return proxy.AddReceiveDestination(correlationID, 9001, channel)
			},
		},
		{
			name: "remove receive destination",
			offer: func(proxy *conductor.DriverProxy, channel string) error {
				return proxy.RemoveReceiveDestination(correlationID, 9001, channel)
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
					require.ErrorIs(t, err, atomicbuffer.ErrStringASCIINonASCII)
					requireZeroDriverProxyCommittedRecords(t, ring)
				})
			}
		})
	}
}

func TestDriverProxy_AddCounter_rejects(t *testing.T) {
	tests := []struct {
		name     string
		key      []byte
		label    string
		fullRing bool
		wantErr  error
	}{
		{
			name:    "keyTooLong",
			key:     make([]byte, cnc.CounterMetadataMaxKeyLength+1),
			wantErr: conductor.ErrCounterKeyTooLong,
		},
		{
			name:    "labelTooLong",
			label:   strings.Repeat("l", cnc.CounterMetadataMaxLabelLength+1),
			wantErr: conductor.ErrCounterLabelTooLong,
		},
		{
			name:    "nonASCIILabel",
			label:   "caf\xc3\xa9",
			wantErr: atomicbuffer.ErrStringASCIINonASCII,
		},
		{
			name:     "keyTooLongFullRing",
			key:      make([]byte, cnc.CounterMetadataMaxKeyLength+1),
			fullRing: true,
			wantErr:  conductor.ErrCounterKeyTooLong,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ring, buffer := newDriverProxyTestRing(t)
			if test.fullRing {
				seedFullDriverProxyRing(t, buffer)
			}
			tailIndex := ringbuffer.TailIndex(driverProxyTestDataCapacity)
			tailBefore := buffer.GetInt64Acquire(tailIndex)
			proxy := conductor.NewDriverProxy(ring, 1)

			err := proxy.AddCounter(42, counterTestTypeID, test.key, test.label)
			require.ErrorIs(t, err, test.wantErr)
			// Validation runs before the claim, so the tail does not move.
			require.Equal(t, tailBefore, buffer.GetInt64Acquire(tailIndex))
			requireZeroDriverProxyCommittedRecords(t, ring)
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
	requireZeroDriverProxyCommittedRecords(t, ring)
}

func TestDriverProxy_ClientClose_insufficientCapacity(t *testing.T) {
	ring, buffer := newDriverProxyTestRing(t)
	seedFullDriverProxyRing(t, buffer)

	proxy := conductor.NewDriverProxy(ring, 1)
	proxy.ClientClose()

	requireZeroDriverProxyCommittedRecords(t, ring)
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
	Encode          func(buf atomicbuffer.AtomicBuffer) error
	WantErr         error
	OutOfBoundsKind string
}

func driverProxyEncodeAtWrongOffset(buf atomicbuffer.AtomicBuffer) int {
	return buf.Capacity()
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
			Encode: func(buf atomicbuffer.AtomicBuffer) error {
				return conductor.EncodeCorrelated(
					buf,
					driverProxyEncodeAtWrongOffset(buf),
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
			Encode: func(buf atomicbuffer.AtomicBuffer) error {
				return conductor.EncodePublicationMessage(
					buf,
					driverProxyEncodeAtWrongOffset(buf),
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
			Encode: func(buf atomicbuffer.AtomicBuffer) error {
				return conductor.EncodeSubscriptionMessage(
					buf,
					driverProxyEncodeAtWrongOffset(buf),
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
			Encode: func(buf atomicbuffer.AtomicBuffer) error {
				return conductor.EncodeRemovePublicationMessage(
					buf,
					driverProxyEncodeAtWrongOffset(buf),
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
			Encode: func(buf atomicbuffer.AtomicBuffer) error {
				return conductor.EncodeRemoveMessage(
					buf,
					driverProxyEncodeAtWrongOffset(buf),
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
		func(_ int) error {
			return encodeCase.Encode(proxy.Ring().Buffer())
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
		func(_ int) error {
			return encodeCase.Encode(proxy.Ring().Buffer())
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

	require.Zero(tb, testutil.CommittedNonPaddingRecordCount(ring), "no committed non-padding record")
}

func newDriverProxyTestRing(
	tb testing.TB,
) (*ringbuffer.ManyToOneRingBuffer, atomicbuffer.AtomicBuffer) {
	tb.Helper()

	buf := make([]byte, ringbuffer.TotalBufferLength(driverProxyTestDataCapacity))
	buffer := atomicbuffer.Wrap(buf)
	ring, err := ringbuffer.NewManyToOneRingBuffer(buffer)
	require.NoError(tb, err)
	return ring, buffer
}

func seedFullDriverProxyRing(tb testing.TB, buffer atomicbuffer.AtomicBuffer) {
	tb.Helper()

	head := int64(0)
	tail := int64(driverProxyTestDataCapacity)
	buffer.PutInt64Release(ringbuffer.HeadIndex(driverProxyTestDataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(driverProxyTestDataCapacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(driverProxyTestDataCapacity), head)
}

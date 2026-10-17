//go:build rapid

package conductor_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

type channelFlyweightKind int

const (
	channelFlyweightPublication channelFlyweightKind = iota
	channelFlyweightSubscription
	channelFlyweightDestination
)

func drawChannelFlyweightKind(rt *rapid.T, name string) channelFlyweightKind {
	return channelFlyweightKind(
		rapid.IntRange(0, int(channelFlyweightDestination)).Draw(rt, name),
	)
}

func encodePublicationMessage(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	msg conductor.PublicationMessage,
) error {
	return conductor.EncodePublicationMessage(
		buffer,
		offset,
		msg.ClientID,
		msg.CorrelationID,
		msg.StreamID,
		msg.Channel,
	)
}

func encodeSubscriptionMessage(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	msg conductor.SubscriptionMessage,
) error {
	return conductor.EncodeSubscriptionMessage(
		buffer,
		offset,
		msg.ClientID,
		msg.CorrelationID,
		msg.RegistrationCorrelationID,
		msg.StreamID,
		msg.Channel,
	)
}

func encodeDestinationMessage(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	msg conductor.DestinationMessage,
) error {
	return conductor.EncodeDestinationMessage(
		buffer,
		offset,
		msg.ClientID,
		msg.CorrelationID,
		msg.RegistrationCorrelationID,
		msg.Channel,
	)
}

func TestChannelFlyweight_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		kind := drawChannelFlyweightKind(rt, "flyweightKind")
		clientID := rapid.Int64().Draw(rt, "clientID")
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		streamID := rapid.Int32().Draw(rt, "streamID")
		channel := testutil.DrawASCIIString(rt, "channel", 256)
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		switch kind {
		case channelFlyweightPublication:
			want := conductor.PublicationMessage{
				ClientID:      clientID,
				CorrelationID: correlationID,
				StreamID:      streamID,
				Channel:       channel,
			}
			requireFlyweightEncodeDecodeRoundtrip(
				rt,
				offset,
				conductor.PublicationMessageBodyLength(channel),
				want,
				encodePublicationMessage,
				conductor.DecodePublicationMessage,
			)
		case channelFlyweightSubscription:
			registrationCorrelationID := rapid.Int64().Draw(rt, "registrationCorrelationID")
			want := conductor.SubscriptionMessage{
				ClientID:                  clientID,
				CorrelationID:             correlationID,
				RegistrationCorrelationID: registrationCorrelationID,
				StreamID:                  streamID,
				Channel:                   channel,
			}
			requireFlyweightEncodeDecodeRoundtrip(
				rt,
				offset,
				conductor.SubscriptionMessageBodyLength(channel),
				want,
				encodeSubscriptionMessage,
				conductor.DecodeSubscriptionMessage,
			)
		case channelFlyweightDestination:
			registrationCorrelationID := rapid.Int64().Draw(rt, "registrationCorrelationID")
			want := conductor.DestinationMessage{
				ClientID:                  clientID,
				CorrelationID:             correlationID,
				RegistrationCorrelationID: registrationCorrelationID,
				Channel:                   channel,
			}
			requireFlyweightEncodeDecodeRoundtrip(
				rt,
				offset,
				conductor.DestinationMessageBodyLength(channel),
				want,
				encodeDestinationMessage,
				conductor.DecodeDestinationMessage,
			)
		}
	})
}

func TestRemoveMessage_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		clientID := rapid.Int64().Draw(rt, "clientID")
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		registrationID := rapid.Int64().Draw(rt, "registrationID")
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		buffer := atomicbuffer.Wrap(make([]byte, offset+conductor.RemoveMessageBodyLength))
		err := conductor.EncodeRemoveMessage(
			buffer,
			offset,
			clientID,
			correlationID,
			registrationID,
		)
		require.NoError(rt, err)

		got, err := conductor.DecodeRemoveMessage(
			buffer,
			offset,
			conductor.RemoveMessageBodyLength,
		)
		require.NoError(rt, err)
		require.Equal(rt, conductor.RemoveMessage{
			ClientID:       clientID,
			CorrelationID:  correlationID,
			RegistrationID: registrationID,
		}, got)
	})
}

func TestRemovePublication_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		clientID := rapid.Int64().Draw(rt, "clientID")
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		registrationID := rapid.Int64().Draw(rt, "registrationID")
		revoke := rapid.Bool().Draw(rt, "revoke")
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		buffer := atomicbuffer.Wrap(make([]byte, offset+conductor.RemovePublicationBodyLength))
		err := conductor.EncodeRemovePublicationMessage(
			buffer,
			offset,
			clientID,
			correlationID,
			registrationID,
			revoke,
		)
		require.NoError(rt, err)

		got, err := conductor.DecodeRemovePublicationMessage(
			buffer,
			offset,
			conductor.RemovePublicationBodyLength,
		)
		require.NoError(rt, err)
		require.Equal(rt, conductor.RemovePublicationMessage{
			RemoveMessage: conductor.RemoveMessage{
				ClientID:       clientID,
				CorrelationID:  correlationID,
				RegistrationID: registrationID,
			},
			Revoke: revoke,
		}, got)

		shortGot, err := conductor.DecodeRemovePublicationMessage(
			buffer,
			offset,
			conductor.RemoveMessageBodyLength,
		)
		require.NoError(rt, err)
		require.False(rt, shortGot.Revoke)
		require.Equal(rt, got.RemoveMessage, shortGot.RemoveMessage)
	})
}

func TestCounterMessage_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		want := conductor.CounterMessage{
			ClientID:      rapid.Int64().Draw(rt, "clientID"),
			CorrelationID: rapid.Int64().Draw(rt, "correlationID"),
			TypeID:        rapid.Int32().Draw(rt, "typeID"),
			Key: append([]byte{}, rapid.SliceOfN(
				rapid.Byte(),
				0,
				cnc.CounterMetadataMaxKeyLength,
			).Draw(rt, "key")...),
			Label: testutil.DrawASCIIString(rt, "label", cnc.CounterMetadataMaxLabelLength),
		}
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")
		trailing := rapid.IntRange(0, 8).Draw(rt, "trailing")

		keyLength := len(want.Key)
		labelLength := len(want.Label)
		alignedKeyLength := (keyLength + 3) &^ 3
		bodyLength := conductor.CounterMessageBodyLength(keyLength, labelLength)
		require.Equal(rt, 28+alignedKeyLength+labelLength, bodyLength)

		labelLengthOffset := conductor.CounterMessageLabelLengthOffset(keyLength)
		require.Zero(rt, labelLengthOffset%4)
		require.Equal(rt, conductor.CounterMessageKeyOffset+alignedKeyLength, labelLengthOffset)

		buffer := atomicbuffer.Wrap(make([]byte, offset+bodyLength+trailing))
		require.NoError(rt, conductor.EncodeCounterMessage(
			buffer,
			offset,
			want.ClientID,
			want.CorrelationID,
			want.TypeID,
			want.Key,
			want.Label,
		))
		require.Equal(rt, int32(labelLength), buffer.GetInt32(offset+labelLengthOffset))

		got, err := conductor.DecodeCounterMessage(buffer, offset, bodyLength+trailing)
		require.NoError(rt, err)
		require.Equal(rt, want, got)

		if bodyLength > conductor.CounterMessageMinimumBodyLength {
			_, err = conductor.DecodeCounterMessage(buffer, offset, bodyLength-1)
			require.ErrorIs(rt, err, conductor.ErrCounterMessageOutOfBounds)
		}
	})
}

func TestDecodeTrailingStringASCII_exactFit_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		flyweightKind := drawChannelFlyweightKind(rt, "flyweightKind")
		clientID := rapid.Int64().Draw(rt, "clientID")
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		streamID := rapid.Int32().Draw(rt, "streamID")
		channel := testutil.DrawASCIIStringLenRange(rt, "channel", 1, 256)
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		var (
			bodyLength   int
			stringOffset int
			wrappedErr   error
			buffer       atomicbuffer.AtomicBuffer
		)
		switch flyweightKind {
		case channelFlyweightPublication:
			bodyLength = conductor.PublicationMessageBodyLength(channel)
			stringOffset = conductor.PublicationMessageChannelOffset
			wrappedErr = conductor.ErrPublicationMessageOutOfBounds
			buffer = atomicbuffer.Wrap(make([]byte, offset+bodyLength+1))
			require.NoError(rt, conductor.EncodePublicationMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				streamID,
				channel,
			))
		case channelFlyweightSubscription:
			registrationCorrelationID := rapid.Int64().Draw(rt, "registrationCorrelationID")
			bodyLength = conductor.SubscriptionMessageBodyLength(channel)
			stringOffset = conductor.SubscriptionMessageChannelOffset
			wrappedErr = conductor.ErrSubscriptionMessageOutOfBounds
			buffer = atomicbuffer.Wrap(make([]byte, offset+bodyLength+1))
			require.NoError(rt, conductor.EncodeSubscriptionMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				registrationCorrelationID,
				streamID,
				channel,
			))
		case channelFlyweightDestination:
			registrationCorrelationID := rapid.Int64().Draw(rt, "registrationCorrelationID")
			bodyLength = conductor.DestinationMessageBodyLength(channel)
			stringOffset = conductor.DestinationMessageChannelOffset
			wrappedErr = conductor.ErrDestinationMessageOutOfBounds
			buffer = atomicbuffer.Wrap(make([]byte, offset+bodyLength+1))
			require.NoError(rt, conductor.EncodeDestinationMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				registrationCorrelationID,
				channel,
			))
		}

		decodeTrailing := func(bodyLen int) (string, error) {
			return conductor.DecodeTrailingStringASCIIForTest(
				buffer,
				offset,
				bodyLen,
				stringOffset,
				wrappedErr,
			)
		}

		got, err := decodeTrailing(bodyLength)
		require.NoError(rt, err)
		require.Equal(rt, channel, got)

		shortBodyLength := bodyLength - 1
		_, err = decodeTrailing(shortBodyLength)
		require.ErrorIs(rt, err, atomicbuffer.ErrStringASCIIOutOfBounds)
		require.EqualError(
			rt,
			err,
			fmt.Sprintf(
				"atomicbuffer: string ascii out of bounds: offset=%d capacity=%d",
				atomicbuffer.StringASCIIHeaderLength,
				shortBodyLength-stringOffset,
			),
		)

		_, err = decodeTrailing(bodyLength + 1)
		require.ErrorIs(rt, err, wrappedErr)
		require.EqualError(
			rt,
			err,
			fmt.Sprintf(
				"conductor: %s message out of bounds: decodedLength=%d bodyLength=%d",
				channelFlyweightMessageName(flyweightKind),
				bodyLength,
				bodyLength+1,
			),
		)
	})
}

func channelFlyweightMessageName(kind channelFlyweightKind) string {
	switch kind {
	case channelFlyweightPublication:
		return "publication"
	case channelFlyweightSubscription:
		return "subscription"
	case channelFlyweightDestination:
		return "destination"
	default:
		return "unknown"
	}
}

package conductor_test

import (
	"bytes"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

type eventFlyweightKind int

const (
	eventFlyweightPublicationBuffersReady eventFlyweightKind = iota
	eventFlyweightSubscriptionReady
	eventFlyweightAvailableImage
	eventFlyweightUnavailableImage
	eventFlyweightPublicationError
)

func drawEventFlyweightKind(rt *rapid.T, name string) eventFlyweightKind {
	return eventFlyweightKind(
		rapid.IntRange(0, int(eventFlyweightPublicationError)).Draw(rt, name),
	)
}

func drawStringBearingEventFlyweightKind(rt *rapid.T, name string) eventFlyweightKind {
	switch rapid.IntRange(0, 3).Draw(rt, name) {
	case 0:
		return eventFlyweightPublicationBuffersReady
	case 1:
		return eventFlyweightAvailableImage
	case 2:
		return eventFlyweightUnavailableImage
	default:
		return eventFlyweightPublicationError
	}
}

func drawPublicationBuffersReady(rt *rapid.T) conductor.PublicationBuffersReady {
	return conductor.PublicationBuffersReady{
		CorrelationID:             rapid.Int64().Draw(rt, "correlationID"),
		RegistrationID:            rapid.Int64().Draw(rt, "registrationID"),
		SessionID:                 rapid.Int32().Draw(rt, "sessionID"),
		StreamID:                  rapid.Int32().Draw(rt, "streamID"),
		PublicationLimitCounterID: rapid.Int32().Draw(rt, "publicationLimitCounterID"),
		ChannelStatusCounterID:    rapid.Int32().Draw(rt, "channelStatusCounterID"),
		LogFileName:               testutil.DrawASCIIString(rt, "logFileName", 256),
	}
}

func drawSubscriptionReady(rt *rapid.T) conductor.SubscriptionReady {
	return conductor.SubscriptionReady{
		CorrelationID:          rapid.Int64().Draw(rt, "correlationID"),
		ChannelStatusCounterID: rapid.Int32().Draw(rt, "channelStatusCounterID"),
	}
}

func drawAvailableImage(rt *rapid.T) conductor.AvailableImage {
	return conductor.AvailableImage{
		CorrelationID:              rapid.Int64().Draw(rt, "correlationID"),
		SessionID:                  rapid.Int32().Draw(rt, "sessionID"),
		StreamID:                   rapid.Int32().Draw(rt, "streamID"),
		SubscriptionRegistrationID: rapid.Int64().Draw(rt, "subscriptionRegistrationID"),
		SubscriberPositionID:       rapid.Int32().Draw(rt, "subscriberPositionID"),
		LogFileName:                testutil.DrawASCIIStringLenRange(rt, "logFileName", 0, 256),
		SourceIdentity:             testutil.DrawASCIIStringLenRange(rt, "sourceIdentity", 0, 256),
	}
}

func drawUnavailableImage(rt *rapid.T) conductor.UnavailableImage {
	return conductor.UnavailableImage{
		CorrelationID:              rapid.Int64().Draw(rt, "correlationID"),
		SubscriptionRegistrationID: rapid.Int64().Draw(rt, "subscriptionRegistrationID"),
		StreamID:                   rapid.Int32().Draw(rt, "streamID"),
		Channel:                    testutil.DrawASCIIString(rt, "channel", 256),
	}
}

func drawPublicationError(rt *rapid.T) conductor.PublicationError {
	addressBytes := rapid.SliceOfN(rapid.Byte(), net.IPv6len, net.IPv6len).Draw(rt, "address")
	var address [net.IPv6len]byte
	copy(address[:], addressBytes)

	return conductor.PublicationError{
		RegistrationID:            rapid.Int64().Draw(rt, "registrationID"),
		DestinationRegistrationID: rapid.Int64().Draw(rt, "destinationRegistrationID"),
		SessionID:                 rapid.Int32().Draw(rt, "sessionID"),
		StreamID:                  rapid.Int32().Draw(rt, "streamID"),
		ReceiverID:                rapid.Int64().Draw(rt, "receiverID"),
		GroupTag:                  rapid.Int64().Draw(rt, "groupTag"),
		AddressType:               rapid.Int16().Draw(rt, "addressType"),
		AddressPort:               rapid.Uint16().Draw(rt, "addressPort"),
		Address:                   address,
		ErrorCode:                 rapid.Int32().Draw(rt, "errorCode"),
		ErrorMessage:              testutil.DrawASCIIString(rt, "errorMessage", 256),
	}
}

func requireEventEncodeDecodeRoundtrip[T any](
	rt *rapid.T,
	offset int,
	bodyLength int,
	want T,
	encode func(atomic.AtomicBuffer, int, T) error,
	decode func(atomic.AtomicBuffer, int, int) (T, error),
) {
	buffer := atomic.Wrap(make([]byte, offset+bodyLength))
	prefix := testutil.SnapshotBufferPrefix(buffer, offset)
	require.NoError(rt, encode(buffer, offset, want))
	require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))

	got, err := decode(buffer, offset, bodyLength)
	require.NoError(rt, err)
	require.Equal(rt, want, got)
}

func requireAvailableImageSourceIdentityAligned(
	rt *rapid.T,
	buffer atomic.AtomicBuffer,
	offset int,
	bodyLength int,
) {
	sourceIdentityOffset, err := conductor.SourceIdentityOffset(
		buffer,
		offset,
		bodyLength,
		conductor.AvailableImageFixedLength,
	)
	require.NoError(rt, err)
	require.Equal(rt, 0, sourceIdentityOffset%4)
}

func padPayloadWithTrailingBytes(payload []byte, trailingExtra int) []byte {
	out := make([]byte, len(payload)+trailingExtra)
	copy(out, payload)
	for i := len(payload); i < len(out); i++ {
		out[i] = 0xFF
	}
	return out
}

func TestEventFlyweight_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		kind := drawEventFlyweightKind(rt, "flyweightKind")
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		switch kind {
		case eventFlyweightPublicationBuffersReady:
			want := drawPublicationBuffersReady(rt)
			bodyLength := conductor.PublicationBuffersReadyBodyLength(want.LogFileName)
			require.Len(rt, conductor.PublicationBuffersReadyBytes(want), bodyLength)
			requireEventEncodeDecodeRoundtrip(
				rt,
				offset,
				bodyLength,
				want,
				conductor.EncodePublicationBuffersReady,
				conductor.DecodePublicationBuffersReady,
			)

		case eventFlyweightSubscriptionReady:
			want := drawSubscriptionReady(rt)
			bodyLength := conductor.SubscriptionReadyBodyLength
			require.Len(rt, conductor.SubscriptionReadyBytes(want), bodyLength)
			requireEventEncodeDecodeRoundtrip(
				rt,
				offset,
				bodyLength,
				want,
				conductor.EncodeSubscriptionReady,
				conductor.DecodeSubscriptionReady,
			)

		case eventFlyweightAvailableImage:
			want := drawAvailableImage(rt)
			bodyLength := conductor.AvailableImageBodyLength(want.LogFileName, want.SourceIdentity)
			require.Len(rt, conductor.AvailableImageBytes(want), bodyLength)

			buffer := atomic.Wrap(make([]byte, offset+bodyLength))
			prefix := testutil.SnapshotBufferPrefix(buffer, offset)
			require.NoError(rt, conductor.EncodeAvailableImage(buffer, offset, want))
			require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))
			requireAvailableImageSourceIdentityAligned(rt, buffer, offset, bodyLength)

			got, err := conductor.DecodeAvailableImage(buffer, offset, bodyLength)
			require.NoError(rt, err)
			require.Equal(rt, want, got)

		case eventFlyweightUnavailableImage:
			want := drawUnavailableImage(rt)
			bodyLength := conductor.UnavailableImageBodyLength(want.Channel)
			require.Len(rt, conductor.UnavailableImageBytes(want), bodyLength)
			requireEventEncodeDecodeRoundtrip(
				rt,
				offset,
				bodyLength,
				want,
				conductor.EncodeUnavailableImage,
				conductor.DecodeUnavailableImage,
			)

		case eventFlyweightPublicationError:
			want := drawPublicationError(rt)
			bodyLength := conductor.PublicationErrorBodyLength(want.ErrorMessage)
			require.Len(rt, conductor.PublicationErrorBytes(want), bodyLength)
			requireEventEncodeDecodeRoundtrip(
				rt,
				offset,
				bodyLength,
				want,
				conductor.EncodePublicationError,
				conductor.DecodePublicationError,
			)
		}
	})
}

func TestEventFlyweight_trailingPadding_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		kind := drawStringBearingEventFlyweightKind(rt, "flyweightKind")
		trailingExtra := rapid.IntRange(1, 8).Draw(rt, "trailingExtra")

		switch kind {
		case eventFlyweightPublicationBuffersReady:
			want := drawPublicationBuffersReady(rt)
			payload := conductor.PublicationBuffersReadyBytes(want)
			padded := padPayloadWithTrailingBytes(payload, trailingExtra)

			got, err := conductor.DecodePublicationBuffersReady(atomic.Wrap(padded), 0, len(padded))
			require.NoError(rt, err)
			require.Equal(rt, want, got)

		case eventFlyweightAvailableImage:
			want := drawAvailableImage(rt)
			payload := conductor.AvailableImageBytes(want)
			padded := padPayloadWithTrailingBytes(payload, trailingExtra)

			got, err := conductor.DecodeAvailableImage(atomic.Wrap(padded), 0, len(padded))
			require.NoError(rt, err)
			require.Equal(rt, want, got)

		case eventFlyweightUnavailableImage:
			want := drawUnavailableImage(rt)
			payload := conductor.UnavailableImageBytes(want)
			padded := padPayloadWithTrailingBytes(payload, trailingExtra)

			got, err := conductor.DecodeUnavailableImage(atomic.Wrap(padded), 0, len(padded))
			require.NoError(rt, err)
			require.Equal(rt, want, got)

		case eventFlyweightPublicationError:
			want := drawPublicationError(rt)
			payload := conductor.PublicationErrorBytes(want)
			padded := padPayloadWithTrailingBytes(payload, trailingExtra)

			got, err := conductor.DecodePublicationError(atomic.Wrap(padded), 0, len(padded))
			require.NoError(rt, err)
			require.Equal(rt, want, got)
		}
	})
}

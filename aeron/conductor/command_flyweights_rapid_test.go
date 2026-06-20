//go:build rapid

package conductor_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/internal/testutil"
)

type channelFlyweightKind int

const (
	channelFlyweightPublication channelFlyweightKind = iota
	channelFlyweightSubscription
)

func drawChannelFlyweightKind(rt *rapid.T, name string) channelFlyweightKind {
	return channelFlyweightKind(
		rapid.IntRange(0, int(channelFlyweightSubscription)).Draw(rt, name),
	)
}

func TestChannelFlyweight_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		kind := drawChannelFlyweightKind(rt, "flyweightKind")
		clientID := rapid.Int64().Draw(rt, "clientID")
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		streamID := rapid.Int32().Draw(rt, "streamID")
		channel := testutil.DrawASCIIString(rt, "channel", 256)
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		switch kind {
		case channelFlyweightPublication:
			bodyLength := conductor.PublicationMessageBodyLength(channel)
			buffer := atomic.Wrap(make([]byte, offset+bodyLength))
			prefix := testutil.SnapshotBufferPrefix(buffer, offset)
			err := conductor.EncodePublicationMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				streamID,
				channel,
			)
			require.NoError(rt, err)
			require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))

			got, err := conductor.DecodePublicationMessage(buffer, offset, bodyLength)
			require.NoError(rt, err)
			require.Equal(rt, conductor.PublicationMessage{
				ClientID:      clientID,
				CorrelationID: correlationID,
				StreamID:      streamID,
				Channel:       channel,
			}, got)
		case channelFlyweightSubscription:
			registrationCorrelationID := rapid.Int64().Draw(rt, "registrationCorrelationID")
			bodyLength := conductor.SubscriptionMessageBodyLength(channel)
			buffer := atomic.Wrap(make([]byte, offset+bodyLength))
			prefix := testutil.SnapshotBufferPrefix(buffer, offset)
			err := conductor.EncodeSubscriptionMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				registrationCorrelationID,
				streamID,
				channel,
			)
			require.NoError(rt, err)
			require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))

			got, err := conductor.DecodeSubscriptionMessage(buffer, offset, bodyLength)
			require.NoError(rt, err)
			require.Equal(rt, conductor.SubscriptionMessage{
				ClientID:                  clientID,
				CorrelationID:             correlationID,
				RegistrationCorrelationID: registrationCorrelationID,
				StreamID:                  streamID,
				Channel:                   channel,
			}, got)
		}
	})
}

func TestRemoveMessage_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		clientID := rapid.Int64().Draw(rt, "clientID")
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		registrationID := rapid.Int64().Draw(rt, "registrationID")
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		buffer := atomic.Wrap(make([]byte, offset+conductor.RemoveMessageBodyLength))
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
	rapid.Check(t, func(rt *rapid.T) {
		clientID := rapid.Int64().Draw(rt, "clientID")
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		registrationID := rapid.Int64().Draw(rt, "registrationID")
		revoke := rapid.Bool().Draw(rt, "revoke")
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		buffer := atomic.Wrap(make([]byte, offset+conductor.RemovePublicationBodyLength))
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

func TestDecodeTrailingStringASCII_exactFit_rapid(t *testing.T) {
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
			buffer       atomic.AtomicBuffer
		)
		switch flyweightKind {
		case channelFlyweightPublication:
			bodyLength = conductor.PublicationMessageBodyLength(channel)
			stringOffset = conductor.PublicationMessageChannelOffset
			wrappedErr = conductor.ErrPublicationMessageOutOfBounds
			buffer = atomic.Wrap(make([]byte, offset+bodyLength+1))
			err := conductor.EncodePublicationMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				streamID,
				channel,
			)
			require.NoError(rt, err)
		case channelFlyweightSubscription:
			registrationCorrelationID := rapid.Int64().Draw(rt, "registrationCorrelationID")
			bodyLength = conductor.SubscriptionMessageBodyLength(channel)
			stringOffset = conductor.SubscriptionMessageChannelOffset
			wrappedErr = conductor.ErrSubscriptionMessageOutOfBounds
			buffer = atomic.Wrap(make([]byte, offset+bodyLength+1))
			err := conductor.EncodeSubscriptionMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				registrationCorrelationID,
				streamID,
				channel,
			)
			require.NoError(rt, err)
		}

		got, err := conductor.DecodeTrailingStringASCIIForTest(
			buffer,
			offset,
			bodyLength,
			stringOffset,
			wrappedErr,
		)
		require.NoError(rt, err)
		require.Equal(rt, channel, got)

		shortBodyLength := bodyLength - 1
		_, err = conductor.DecodeTrailingStringASCIIForTest(
			buffer,
			offset,
			shortBodyLength,
			stringOffset,
			wrappedErr,
		)
		require.ErrorIs(rt, err, atomic.ErrStringASCIIOutOfBounds)
		require.EqualError(
			rt,
			err,
			fmt.Sprintf(
				"atomic: string ascii out of bounds: offset=%d capacity=%d",
				atomic.StringASCIIHeaderLength,
				shortBodyLength-stringOffset,
			),
		)

		_, err = conductor.DecodeTrailingStringASCIIForTest(
			buffer,
			offset,
			bodyLength+1,
			stringOffset,
			wrappedErr,
		)
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
	default:
		return "unknown"
	}
}

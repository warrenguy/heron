package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
)

func subscriptionMessageIPCBytes() []byte {
	return conductor.SubscriptionMessageBytes(
		1,
		42,
		conductor.NullRegistrationCorrelationID,
		1001,
		"aeron:ipc",
	)
}

func subscriptionMessageHarness() streamChannelMessageHarness {
	return streamChannelMessageHarness{
		minimumBodyLength: conductor.SubscriptionMessageMinimumBodyLength,
		errOutOfBounds:    conductor.ErrSubscriptionMessageOutOfBounds,
		bodyLength:        conductor.SubscriptionMessageBodyLength,
		encode: func(
			buffer atomic.AtomicBuffer,
			offset int,
			clientID int64,
			correlationID int64,
			streamID int32,
			channel string,
		) error {
			return conductor.EncodeSubscriptionMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				conductor.NullRegistrationCorrelationID,
				streamID,
				channel,
			)
		},
		decode: func(
			buffer atomic.AtomicBuffer,
			offset int,
			bodyLength int,
		) (int64, int64, int32, string, error) {
			got, err := conductor.DecodeSubscriptionMessage(buffer, offset, bodyLength)
			if err != nil {
				return 0, 0, 0, "", err
			}
			return got.ClientID, got.CorrelationID, got.StreamID, got.Channel, nil
		},
	}
}

func TestSubscriptionMessageRoundtrip(t *testing.T) {
	tests := []struct {
		name                      string
		clientID                  int64
		correlationID             int64
		registrationCorrelationID int64
		streamID                  int32
		channel                   string
	}{
		{
			name:                      "null registration correlation id",
			clientID:                  1,
			correlationID:             42,
			registrationCorrelationID: conductor.NullRegistrationCorrelationID,
			streamID:                  1001,
			channel:                   "aeron:ipc",
		},
		{
			name:                      "non-zero registration correlation id",
			clientID:                  2,
			correlationID:             77,
			registrationCorrelationID: 9001,
			streamID:                  55,
			channel:                   "aeron:udp?endpoint=localhost:40123",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bodyLength := conductor.SubscriptionMessageBodyLength(test.channel)
			buffer := atomic.Wrap(make([]byte, bodyLength))
			err := conductor.EncodeSubscriptionMessage(
				buffer,
				0,
				test.clientID,
				test.correlationID,
				test.registrationCorrelationID,
				test.streamID,
				test.channel,
			)
			require.NoError(t, err)

			got, err := conductor.DecodeSubscriptionMessage(buffer, 0, bodyLength)
			require.NoError(t, err)
			require.Equal(t, conductor.SubscriptionMessage{
				ClientID:                  test.clientID,
				CorrelationID:             test.correlationID,
				RegistrationCorrelationID: test.registrationCorrelationID,
				StreamID:                  test.streamID,
				Channel:                   test.channel,
			}, got)
		})
	}
}

func TestSubscriptionMessageBytes_golden(t *testing.T) {
	require.Equal(
		t,
		[]byte{
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xe9, 0x03, 0x00, 0x00,
			0x09, 0x00, 0x00, 0x00,
			0x61, 0x65, 0x72, 0x6f, 0x6e, 0x3a, 0x69, 0x70, 0x63,
		},
		subscriptionMessageIPCBytes(),
	)
}

func TestSubscriptionMessageBodyLength(t *testing.T) {
	testStreamChannelMessageBodyLength(t, subscriptionMessageHarness())
}

func TestDecodeSubscriptionMessage_bodyTooShort(t *testing.T) {
	testDecodeStreamChannelMessageBodyTooShort(t, subscriptionMessageHarness(), 28)
}

func TestDecodeSubscriptionMessage_channelTruncated(t *testing.T) {
	testDecodeStreamChannelMessageChannelTruncated(
		t,
		subscriptionMessageHarness(),
		subscriptionMessageIPCBytes(),
		36,
		"atomic: string ascii out of bounds: offset=4 capacity=8",
	)
}

func TestDecodeSubscriptionMessage_bodyLongerThanDecoded(t *testing.T) {
	testDecodeStreamChannelMessageDecodedLengthMismatch(
		t,
		subscriptionMessageHarness(),
		subscriptionMessageIPCBytes(),
		43,
		41,
	)
}

func TestEncodeSubscriptionMessage_outOfBounds(t *testing.T) {
	testEncodeStreamChannelMessageOutOfBounds(t, subscriptionMessageHarness())
}

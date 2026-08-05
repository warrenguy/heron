package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
)

func destinationMessageIPCBytes() []byte {
	return conductor.DestinationMessageBytes(
		1,
		42,
		9001,
		"aeron:ipc",
	)
}

// destinationMessageHarness adapts DestinationMessage to the stream-channel
// bounds helpers. Encode uses a fixed registration correlation id. Decode
// reports streamID 0: the destination body has no stream field.
func destinationMessageHarness() streamChannelMessageHarness {
	const registrationCorrelationID int64 = 9001
	return streamChannelMessageHarness{
		minimumBodyLength: conductor.DestinationMessageMinimumBodyLength,
		errOutOfBounds:    conductor.ErrDestinationMessageOutOfBounds,
		bodyLength:        conductor.DestinationMessageBodyLength,
		encode: func(
			buffer atomicbuffer.AtomicBuffer,
			offset int,
			clientID int64,
			correlationID int64,
			_ int32,
			channel string,
		) error {
			return conductor.EncodeDestinationMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				registrationCorrelationID,
				channel,
			)
		},
		decode: func(
			buffer atomicbuffer.AtomicBuffer,
			offset int,
			bodyLength int,
		) (int64, int64, int32, string, error) {
			got, err := conductor.DecodeDestinationMessage(buffer, offset, bodyLength)
			if err != nil {
				return 0, 0, 0, "", err
			}
			return got.ClientID, got.CorrelationID, 0, got.Channel, nil
		},
	}
}

func TestDestinationMessageRoundtrip(t *testing.T) {
	tests := []struct {
		name                      string
		clientID                  int64
		correlationID             int64
		registrationCorrelationID int64
		channel                   string
	}{
		{
			name:                      "ipc channel",
			clientID:                  1,
			correlationID:             42,
			registrationCorrelationID: 9001,
			channel:                   "aeron:ipc",
		},
		{
			name:                      "udp destination",
			clientID:                  2,
			correlationID:             77,
			registrationCorrelationID: 1001,
			channel:                   "aeron:udp?endpoint=localhost:40123",
		},
		{
			name:                      "empty channel",
			clientID:                  3,
			correlationID:             9,
			registrationCorrelationID: 0,
			channel:                   "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bodyLength := conductor.DestinationMessageBodyLength(test.channel)
			buffer := atomicbuffer.Wrap(make([]byte, bodyLength))
			err := conductor.EncodeDestinationMessage(
				buffer,
				0,
				test.clientID,
				test.correlationID,
				test.registrationCorrelationID,
				test.channel,
			)
			require.NoError(t, err)

			got, err := conductor.DecodeDestinationMessage(buffer, 0, bodyLength)
			require.NoError(t, err)
			require.Equal(t, conductor.DestinationMessage{
				ClientID:                  test.clientID,
				CorrelationID:             test.correlationID,
				RegistrationCorrelationID: test.registrationCorrelationID,
				Channel:                   test.channel,
			}, got)
		})
	}
}

func TestDestinationMessageBytes_golden(t *testing.T) {
	// clientID=1, correlationID=42, registrationCorrelationID=9001, channel=aeron:ipc
	require.Equal(
		t,
		[]byte{
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x29, 0x23, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x09, 0x00, 0x00, 0x00,
			0x61, 0x65, 0x72, 0x6f, 0x6e, 0x3a, 0x69, 0x70, 0x63,
		},
		destinationMessageIPCBytes(),
	)
}

func TestDestinationMessageBodyLength(t *testing.T) {
	testStreamChannelMessageBodyLength(t, destinationMessageHarness())
}

func TestDecodeDestinationMessage_bodyTooShort(t *testing.T) {
	testDecodeStreamChannelMessageBodyTooShort(t, destinationMessageHarness(), 24)
}

func TestDecodeDestinationMessage_channelTruncated(t *testing.T) {
	// Fixed prefix 28 plus a partial channel. String header at offset 24 needs
	// 4+9 bytes.
	testDecodeStreamChannelMessageChannelTruncated(
		t,
		destinationMessageHarness(),
		destinationMessageIPCBytes(),
		32,
		"atomicbuffer: string ascii out of bounds: offset=4 capacity=8",
	)
}

func TestDecodeDestinationMessage_bodyLongerThanDecoded(t *testing.T) {
	payload := destinationMessageIPCBytes()
	testDecodeStreamChannelMessageDecodedLengthMismatch(
		t,
		destinationMessageHarness(),
		payload,
		len(payload)+2,
		len(payload),
	)
}

func TestEncodeDestinationMessage_outOfBounds(t *testing.T) {
	testEncodeStreamChannelMessageOutOfBounds(t, destinationMessageHarness())
}

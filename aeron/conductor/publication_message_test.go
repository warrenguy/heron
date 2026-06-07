package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
)

func publicationMessageHarness() streamChannelMessageHarness {
	return streamChannelMessageHarness{
		minimumBodyLength: conductor.PublicationMessageMinimumBodyLength,
		errOutOfBounds:    conductor.ErrPublicationMessageOutOfBounds,
		bodyLength:        conductor.PublicationMessageBodyLength,
		encode: func(
			buffer atomic.AtomicBuffer,
			offset int,
			clientID int64,
			correlationID int64,
			streamID int32,
			channel string,
		) error {
			return conductor.EncodePublicationMessage(
				buffer,
				offset,
				clientID,
				correlationID,
				streamID,
				channel,
			)
		},
		decode: func(
			buffer atomic.AtomicBuffer,
			offset int,
			bodyLength int,
		) (int64, int64, int32, string, error) {
			got, err := conductor.DecodePublicationMessage(buffer, offset, bodyLength)
			if err != nil {
				return 0, 0, 0, "", err
			}
			return got.ClientID, got.CorrelationID, got.StreamID, got.Channel, nil
		},
		bytes: conductor.PublicationMessageBytes,
	}
}

func TestPublicationMessageRoundtrip(t *testing.T) {
	testStreamChannelMessageRoundtrip(t, publicationMessageHarness(), []struct {
		name          string
		offset        int
		clientID      int64
		correlationID int64
		streamID      int32
		channel       string
	}{
		{
			name:          "offset zero",
			offset:        0,
			clientID:      1,
			correlationID: 42,
			streamID:      1001,
			channel:       "aeron:ipc",
		},
		{
			name:          "offset eight",
			offset:        8,
			clientID:      7,
			correlationID: 99,
			streamID:      200,
			channel:       "aeron:ipc",
		},
		{
			name:          "udp channel",
			offset:        0,
			clientID:      3,
			correlationID: 11,
			streamID:      55,
			channel:       "aeron:udp?endpoint=localhost:40123",
		},
	})
}

func TestPublicationMessageBytes_golden(t *testing.T) {
	require.Equal(
		t,
		[]byte{
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0xe9, 0x03, 0x00, 0x00,
			0x09, 0x00, 0x00, 0x00,
			0x61, 0x65, 0x72, 0x6f, 0x6e, 0x3a, 0x69, 0x70, 0x63,
		},
		conductor.PublicationMessageBytes(1, 42, 1001, "aeron:ipc"),
	)
}

func TestPublicationMessageBodyLength(t *testing.T) {
	testStreamChannelMessageBodyLength(t, publicationMessageHarness())
}

func TestDecodePublicationMessage_bodyTooShort(t *testing.T) {
	testDecodeStreamChannelMessageBodyTooShort(t, publicationMessageHarness(), 20)
}

func TestDecodePublicationMessage_channelTruncated(t *testing.T) {
	harness := publicationMessageHarness()
	testDecodeStreamChannelMessageChannelTruncated(
		t,
		harness,
		harness.bytes(1, 42, 1001, "aeron:ipc"),
		30,
		"atomic: string ascii out of bounds: offset=4 capacity=10",
	)
}

func TestDecodePublicationMessage_decodedLengthMismatch(t *testing.T) {
	harness := publicationMessageHarness()
	testDecodeStreamChannelMessageDecodedLengthMismatch(
		t,
		harness,
		harness.bytes(1, 42, 1001, "aeron:ipc"),
		35,
		33,
	)
}

func TestEncodePublicationMessage_outOfBounds(t *testing.T) {
	testEncodeStreamChannelMessageOutOfBounds(t, publicationMessageHarness())
}

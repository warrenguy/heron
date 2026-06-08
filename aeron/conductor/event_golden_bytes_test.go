package conductor

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
)

func TestPublicationBuffersReadyBytes_golden(t *testing.T) {
	const goldenHex = "e903000000000000d2070000000000002a000000070000000a0000000b000000" +
		"1e0000002f6465762f73686d2f6165726f6e2d7075626c69636174696f6e2e6c6f67"
	payload, err := hex.DecodeString(goldenHex)
	require.NoError(t, err)

	event := PublicationBuffersReady{
		CorrelationID:             1001,
		RegistrationID:            2002,
		SessionID:                 42,
		StreamID:                  7,
		PublicationLimitCounterID: 10,
		ChannelStatusCounterID:    11,
		LogFileName:               "/dev/shm/aeron-publication.log",
	}
	require.Equal(t, PublicationBuffersReadyBytes(event), payload)

	decoded, err := DecodePublicationBuffersReady(atomic.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event, decoded)
}

func TestSubscriptionReadyBytes_golden(t *testing.T) {
	const goldenHex = "bb0b0000000000000c000000"
	payload, err := hex.DecodeString(goldenHex)
	require.NoError(t, err)

	event := SubscriptionReady{CorrelationID: 3003, ChannelStatusCounterID: 12}
	require.Equal(t, SubscriptionReadyBytes(event), payload)

	decoded, err := DecodeSubscriptionReady(atomic.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event, decoded)
}

func TestAvailableImageBytes_golden(t *testing.T) {
	const goldenHex = "a40f0000000000002a000000070000008d130000000000000d000000" +
		"1e0000002f6465762f73686d2f6165726f6e2d7075626c69636174696f6e2e6c6f67" +
		"000003000000697063"
	payload, err := hex.DecodeString(goldenHex)
	require.NoError(t, err)

	event := AvailableImage{
		CorrelationID:              4004,
		SessionID:                  42,
		StreamID:                   7,
		SubscriptionRegistrationID: 5005,
		SubscriberPositionID:       13,
		LogFileName:                "/dev/shm/aeron-publication.log",
		SourceIdentity:             "ipc",
	}
	require.Equal(t, AvailableImageBytes(event), payload)

	decoded, err := DecodeAvailableImage(atomic.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event, decoded)
}

func TestUnavailableImageBytes_golden(t *testing.T) {
	const goldenHex = "76170000000000008d1300000000000007000000090000006165726f6e3a697063"
	payload, err := hex.DecodeString(goldenHex)
	require.NoError(t, err)

	event := UnavailableImage{
		CorrelationID:              6006,
		SubscriptionRegistrationID: 5005,
		StreamID:                   7,
		Channel:                    "aeron:ipc",
	}
	require.Equal(t, UnavailableImageBytes(event), payload)

	decoded, err := DecodeUnavailableImage(atomic.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event, decoded)
}

func TestPublicationErrorBytes_golden(t *testing.T) {
	const goldenHex = "d20700000000000000000000000000002a000000070000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000001000000" +
		"0900000073696d756c61746564"
	payload, err := hex.DecodeString(goldenHex)
	require.NoError(t, err)

	event := PublicationError{
		RegistrationID: 2002,
		SessionID:      42,
		StreamID:       7,
		ErrorCode:      1,
		ErrorMessage:   "simulated",
	}
	require.Equal(t, PublicationErrorBytes(event), payload)

	decoded, err := DecodePublicationError(atomic.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event, decoded)
}

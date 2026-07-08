package conductor

import (
	"encoding/hex"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
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

	decoded, err := DecodePublicationBuffersReady(atomicbuffer.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event, decoded)
}

func TestSubscriptionReadyBytes_golden(t *testing.T) {
	const goldenHex = "bb0b0000000000000c000000"
	payload, err := hex.DecodeString(goldenHex)
	require.NoError(t, err)

	event := SubscriptionReady{CorrelationID: 3003, ChannelStatusCounterID: 12}
	require.Equal(t, SubscriptionReadyBytes(event), payload)

	decoded, err := DecodeSubscriptionReady(atomicbuffer.Wrap(payload), 0, len(payload))
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

	decoded, err := DecodeAvailableImage(atomicbuffer.Wrap(payload), 0, len(payload))
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

	decoded, err := DecodeUnavailableImage(atomicbuffer.Wrap(payload), 0, len(payload))
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

	decoded, err := DecodePublicationError(atomicbuffer.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event, decoded)
}

func TestPublicationErrorBytes_nonZeroScalars_golden(t *testing.T) {
	const goldenHex = "d207000000000000bb0b0000000000002a00000007000000" +
		"a40f0000000000008d130000000000000200901f20010db8000000000000000000000001" +
		"010000000900000073696d756c61746564"
	payload, err := hex.DecodeString(goldenHex)
	require.NoError(t, err)

	var address [net.IPv6len]byte
	address[0] = 0x20
	address[1] = 0x01
	address[2] = 0x0d
	address[3] = 0xb8
	address[15] = 0x01

	event := PublicationError{
		RegistrationID:            2002,
		DestinationRegistrationID: 3003,
		SessionID:                 42,
		StreamID:                  7,
		ReceiverID:                4004,
		GroupTag:                  5005,
		AddressType:               2,
		AddressPort:               8080,
		Address:                   address,
		ErrorCode:                 1,
		ErrorMessage:              "simulated",
	}
	require.Equal(t, PublicationErrorBytes(event), payload)

	decoded, err := DecodePublicationError(atomicbuffer.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event, decoded)
}

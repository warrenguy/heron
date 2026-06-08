package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
)

func TestDecodePublicationError_shortBody(t *testing.T) {
	minimum := PublicationErrorFixedLength + atomic.StringASCIIHeaderLength

	testDecodeEventShortBody(
		t,
		minimum,
		[]shortBodyTestCase{
			{name: "fixedMinusOne", bodyLength: PublicationErrorFixedLength - 1},
			{name: "exactFixed", bodyLength: PublicationErrorFixedLength},
			{name: "shortStringPrefix", bodyLength: minimum - 1},
		},
		DecodePublicationError,
	)
}

func TestDecodePublicationError_negativeErrorMessageLength(t *testing.T) {
	testDecodeEventNegativeStringLength(t, PublicationErrorFixedLength, DecodePublicationError)
}

func TestDecodePublicationError_truncatedErrorMessage(t *testing.T) {
	event := PublicationError{
		RegistrationID: 1,
		ErrorMessage:   "abcdef",
	}
	payload := truncateEncodedPayload(t, PublicationErrorBytes(event), 2)

	_, err := DecodePublicationError(atomic.Wrap(payload), 0, len(payload))
	require.ErrorIs(t, err, atomic.ErrStringASCIIOutOfBounds)
}

func TestDecodePublicationError_bodyLongerThanDecoded(t *testing.T) {
	event := PublicationError{
		RegistrationID: 1,
		ErrorMessage:   "x",
	}
	decoded := testDecodeEventAllowsTrailingPadding(
		t,
		PublicationErrorBytes(event),
		DecodePublicationError,
	)
	require.Equal(t, event.ErrorMessage, decoded.ErrorMessage)
}

func TestDecodePublicationError_ipv4AddressBlock(t *testing.T) {
	event := PublicationError{
		RegistrationID: 1,
		AddressType:    1,
		AddressPort:    8080,
		ErrorMessage:   "x",
	}
	event.Address[0] = 127
	event.Address[1] = 0
	event.Address[2] = 0
	event.Address[3] = 1
	payload := PublicationErrorBytes(event)

	decoded, err := DecodePublicationError(atomic.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, int16(1), decoded.AddressType)
	require.Equal(t, uint16(8080), decoded.AddressPort)
	require.Equal(t, event.Address, decoded.Address)
}

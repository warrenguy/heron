package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
)

func TestDecodePublicationBuffersReady_shortBody(t *testing.T) {
	minimum := PublicationBuffersReadyFixedLength + atomic.StringASCIIHeaderLength

	testDecodeEventShortBody(
		t,
		minimum,
		[]shortBodyTestCase{
			{name: "fixedMinusOne", bodyLength: PublicationBuffersReadyFixedLength - 1},
			{name: "exactFixed", bodyLength: PublicationBuffersReadyFixedLength},
			{name: "shortStringPrefix", bodyLength: minimum - 1},
		},
		DecodePublicationBuffersReady,
	)
}

func TestDecodePublicationBuffersReady_bodyLongerThanDecoded(t *testing.T) {
	event := PublicationBuffersReady{
		CorrelationID:  1,
		RegistrationID: 2,
		LogFileName:    "x",
	}
	decoded := testDecodeEventAllowsTrailingPadding(
		t,
		PublicationBuffersReadyBytes(event),
		DecodePublicationBuffersReady,
	)
	require.Equal(t, event.LogFileName, decoded.LogFileName)
}

func TestDecodePublicationBuffersReady_negativeLogFileNameLength(t *testing.T) {
	testDecodeEventNegativeStringLength(
		t,
		PublicationBuffersReadyFixedLength,
		DecodePublicationBuffersReady,
	)
}

func TestDecodePublicationBuffersReady_truncatedLogFileName(t *testing.T) {
	event := PublicationBuffersReady{
		CorrelationID:  1,
		RegistrationID: 2,
		LogFileName:    "abcdef",
	}
	payload := truncateEncodedPayload(t, PublicationBuffersReadyBytes(event), 2)

	_, err := DecodePublicationBuffersReady(atomic.Wrap(payload), 0, len(payload))
	require.ErrorIs(t, err, atomic.ErrStringASCIIOutOfBounds)
}

func TestDecodePublicationBuffersReady_maxLengthLogPath(t *testing.T) {
	logFileName := make([]byte, 256)
	for i := range logFileName {
		logFileName[i] = 'a'
	}
	event := PublicationBuffersReady{
		CorrelationID:  1,
		RegistrationID: 2,
		LogFileName:    string(logFileName),
	}
	payload := PublicationBuffersReadyBytes(event)

	decoded, err := DecodePublicationBuffersReady(atomic.Wrap(payload), 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event.LogFileName, decoded.LogFileName)
}

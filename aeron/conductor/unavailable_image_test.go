package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/agrona/atomic"
)

func TestDecodeUnavailableImage_shortBody(t *testing.T) {
	minimum := UnavailableImageFixedLength + atomic.StringASCIIHeaderLength

	testDecodeEventShortBody(
		t,
		minimum,
		[]shortBodyTestCase{
			{name: "fixedMinusOne", bodyLength: UnavailableImageFixedLength - 1},
			{name: "exactFixed", bodyLength: UnavailableImageFixedLength},
			{name: "shortStringPrefix", bodyLength: minimum - 1},
		},
		DecodeUnavailableImage,
	)
}

func TestDecodeUnavailableImage_negativeChannelLength(t *testing.T) {
	testDecodeEventNegativeStringLength(t, UnavailableImageFixedLength, DecodeUnavailableImage)
}

func TestDecodeUnavailableImage_bodyLongerThanDecoded(t *testing.T) {
	event := UnavailableImage{
		CorrelationID: 1,
		StreamID:      2,
		Channel:       "ipc",
	}
	decoded := testDecodeEventAllowsTrailingPadding(
		t,
		UnavailableImageBytes(event),
		DecodeUnavailableImage,
	)
	require.Equal(t, event.Channel, decoded.Channel)
}

func TestDecodeUnavailableImage_truncatedChannel(t *testing.T) {
	event := UnavailableImage{
		CorrelationID: 1,
		StreamID:      2,
		Channel:       "abcdef",
	}
	payload := truncateEncodedPayload(t, UnavailableImageBytes(event), 2)

	_, err := DecodeUnavailableImage(atomic.Wrap(payload), 0, len(payload))
	require.ErrorIs(t, err, atomic.ErrStringASCIIOutOfBounds)
}

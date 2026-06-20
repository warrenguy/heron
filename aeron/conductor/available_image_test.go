package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/agrona/atomic"
)

func TestDecodeAvailableImage_sourceIdentityAligned(t *testing.T) {
	const logFileName = "foo"
	event := AvailableImage{
		CorrelationID:  1,
		LogFileName:    logFileName,
		SourceIdentity: "bar",
	}
	payload := AvailableImageBytes(event)
	buffer := atomic.Wrap(payload)

	sourceIdentityOffset, err := SourceIdentityOffset(
		buffer,
		0,
		len(payload),
		AvailableImageFixedLength,
	)
	require.NoError(t, err)
	require.Equal(t, 36, sourceIdentityOffset)

	decoded, err := DecodeAvailableImage(buffer, 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, event.SourceIdentity, decoded.SourceIdentity)
}

func TestDecodeAvailableImage_shortBody(t *testing.T) {
	minimum := AvailableImageFixedLength + atomic.StringASCIIHeaderLength

	testDecodeEventShortBody(
		t,
		minimum,
		[]shortBodyTestCase{
			{name: "fixedMinusOne", bodyLength: AvailableImageFixedLength - 1},
			{name: "exactFixed", bodyLength: AvailableImageFixedLength},
			{name: "shortStringPrefix", bodyLength: minimum - 1},
		},
		DecodeAvailableImage,
	)
}

func TestDecodeAvailableImage_negativeLogFileNameLength(t *testing.T) {
	testDecodeEventNegativeStringLength(t, AvailableImageFixedLength, DecodeAvailableImage)
}

func TestDecodeAvailableImage_truncatedLogFileName(t *testing.T) {
	event := AvailableImage{
		CorrelationID:  1,
		LogFileName:    "abcdef",
		SourceIdentity: "ipc",
	}
	payload := truncateEncodedPayload(t, AvailableImageBytes(event), 1)

	_, err := DecodeAvailableImage(atomic.Wrap(payload), 0, len(payload))
	require.ErrorIs(t, err, atomic.ErrStringASCIIOutOfBounds)
}

func TestDecodeAvailableImage_truncatedSourceIdentity(t *testing.T) {
	event := AvailableImage{
		CorrelationID:  1,
		LogFileName:    "ab",
		SourceIdentity: "identity",
	}
	payload := truncateEncodedPayload(t, AvailableImageBytes(event), 2)

	_, err := DecodeAvailableImage(atomic.Wrap(payload), 0, len(payload))
	require.ErrorIs(t, err, atomic.ErrStringASCIIOutOfBounds)
}

func TestDecodeAvailableImage_bodyLongerThanDecoded(t *testing.T) {
	event := AvailableImage{
		CorrelationID:  1,
		LogFileName:    "ab",
		SourceIdentity: "ipc",
	}
	decoded := testDecodeEventAllowsTrailingPadding(
		t,
		AvailableImageBytes(event),
		DecodeAvailableImage,
	)
	require.Equal(t, event.SourceIdentity, decoded.SourceIdentity)
}

func TestDecodeAvailableImage_negativeSourceIdentityLength(t *testing.T) {
	event := AvailableImage{
		CorrelationID:  1,
		LogFileName:    "ab",
		SourceIdentity: "ipc",
	}
	payload := AvailableImageBytes(event)
	sourceIdentityOffset, err := SourceIdentityOffset(
		atomic.Wrap(payload),
		0,
		len(payload),
		AvailableImageFixedLength,
	)
	require.NoError(t, err)
	payload = putNegativeStringLengthAtFieldOffset(payload, sourceIdentityOffset)

	_, err = DecodeAvailableImage(atomic.Wrap(payload), 0, len(payload))
	require.ErrorIs(t, err, atomic.ErrStringASCIILengthInvalid)
}

func TestSourceIdentityOffset_outOfBounds(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, AvailableImageFixedLength))

	_, err := SourceIdentityOffset(buffer, 0, len(buffer.Bytes()), AvailableImageFixedLength)
	require.ErrorIs(t, err, ErrEventBodyTooShort)
}

func TestSourceIdentityOffset_bodyLengthExceedsBuffer(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 32))

	_, err := SourceIdentityOffset(buffer, 0, 36, AvailableImageFixedLength)
	require.ErrorIs(t, err, ErrEventBodyTooShort)
}

func TestSourceIdentityOffset_insufficientHeaderRoom(t *testing.T) {
	event := AvailableImage{
		CorrelationID:  1,
		LogFileName:    "",
		SourceIdentity: "",
	}
	payload := AvailableImageBytes(event)
	require.Len(t, payload, 36)

	_, err := SourceIdentityOffset(atomic.Wrap(payload[:32]), 0, 32, AvailableImageFixedLength)
	require.ErrorIs(t, err, ErrEventBodyTooShort)
}

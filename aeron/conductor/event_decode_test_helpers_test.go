package conductor

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
)

func requireEventBodyTooShortMessage(
	t *testing.T,
	err error,
	bodyLength, minimum int,
) {
	t.Helper()
	require.ErrorIs(t, err, ErrEventBodyTooShort)
	require.EqualError(
		t,
		err,
		"conductor: event body too short: bodyLength="+
			strconv.Itoa(bodyLength)+
			" minimum="+
			strconv.Itoa(minimum),
	)
}

func putNegativeStringLengthPrefix(fixedLength int) []byte {
	return putNegativeStringLengthAtFieldOffset(
		make([]byte, fixedLength+atomic.StringASCIIHeaderLength),
		fixedLength,
	)
}

func putNegativeStringLengthAtFieldOffset(payload []byte, fieldOffset int) []byte {
	buffer := atomic.Wrap(payload)
	buffer.PutInt32(fieldOffset, -1)
	return payload
}

type shortBodyTestCase struct {
	name       string
	bodyLength int
}

func testDecodeEventShortBody[T any](
	t *testing.T,
	minimum int,
	cases []shortBodyTestCase,
	decode func(atomic.AtomicBuffer, int, int) (T, error),
) {
	t.Helper()

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			payload := make([]byte, testCase.bodyLength)

			_, err := decode(atomic.Wrap(payload), 0, len(payload))
			requireEventBodyTooShortMessage(t, err, testCase.bodyLength, minimum)
		})
	}
}

func testDecodeEventNegativeStringLength[T any](
	t *testing.T,
	fixedLength int,
	decode func(atomic.AtomicBuffer, int, int) (T, error),
) {
	t.Helper()

	payload := putNegativeStringLengthPrefix(fixedLength)

	_, err := decode(atomic.Wrap(payload), 0, len(payload))
	require.ErrorIs(t, err, atomic.ErrStringASCIILengthInvalid)
}

func testDecodeEventAllowsTrailingPadding[T any](
	t *testing.T,
	payload []byte,
	decode func(atomic.AtomicBuffer, int, int) (T, error),
) T {
	t.Helper()

	padded := append(append([]byte(nil), payload...), 0xFF)

	decoded, err := decode(atomic.Wrap(padded), 0, len(padded))
	require.NoError(t, err)
	return decoded
}

func truncateEncodedPayload(t *testing.T, payload []byte, trim int) []byte {
	t.Helper()
	require.Greater(t, len(payload), trim)
	return payload[:len(payload)-trim]
}

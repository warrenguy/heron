package conductor

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
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
		make([]byte, fixedLength+atomicbuffer.StringASCIIHeaderLength),
		fixedLength,
	)
}

func putNegativeStringLengthAtFieldOffset(payload []byte, fieldOffset int) []byte {
	buffer := atomicbuffer.Wrap(payload)
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
	decode func(atomicbuffer.AtomicBuffer, int, int) (T, error),
) {
	t.Helper()

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			payload := make([]byte, testCase.bodyLength)

			_, err := decode(atomicbuffer.Wrap(payload), 0, len(payload))
			requireEventBodyTooShortMessage(t, err, testCase.bodyLength, minimum)
		})
	}
}

func testDecodeEventNegativeStringLength[T any](
	t *testing.T,
	fixedLength int,
	decode func(atomicbuffer.AtomicBuffer, int, int) (T, error),
) {
	t.Helper()

	payload := putNegativeStringLengthPrefix(fixedLength)

	_, err := decode(atomicbuffer.Wrap(payload), 0, len(payload))
	require.ErrorIs(t, err, atomicbuffer.ErrStringASCIILengthInvalid)
}

func testDecodeEventAllowsTrailingPadding[T any](
	t *testing.T,
	payload []byte,
	decode func(atomicbuffer.AtomicBuffer, int, int) (T, error),
) T {
	t.Helper()

	padded := append(append([]byte(nil), payload...), 0xFF)

	decoded, err := decode(atomicbuffer.Wrap(padded), 0, len(padded))
	require.NoError(t, err)
	return decoded
}

func truncateEncodedPayload(t *testing.T, payload []byte, trim int) []byte {
	t.Helper()
	require.Greater(t, len(payload), trim)
	return payload[:len(payload)-trim]
}

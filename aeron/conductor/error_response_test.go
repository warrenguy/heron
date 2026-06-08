package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
)

func TestDecodeErrorResponse_golden(t *testing.T) {
	payload := conductor.ErrorResponseBytes(7, 11, "fail")
	buffer := atomic.Wrap(payload)

	got, err := conductor.DecodeErrorResponse(buffer, 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, conductor.ErrorResponse{
		OffendingCommandCorrelationID: 7,
		ErrorCode:                     11,
		ErrorMessage:                  "fail",
	}, got)
}

func TestDecodeErrorResponse_emptyMessage(t *testing.T) {
	payload := conductor.ErrorResponseBytes(7, 0, "")
	buffer := atomic.Wrap(payload)

	got, err := conductor.DecodeErrorResponse(buffer, 0, len(payload))
	require.NoError(t, err)
	require.Equal(t, conductor.ErrorResponse{
		OffendingCommandCorrelationID: 7,
		ErrorCode:                     0,
		ErrorMessage:                  "",
	}, got)
}

func TestDecodeErrorResponse_tooShort(t *testing.T) {
	buffer := atomic.Wrap([]byte{0x01, 0x02, 0x03})

	_, err := conductor.DecodeErrorResponse(buffer, 0, 3)
	require.ErrorIs(t, err, conductor.ErrErrorResponseTooShort)
	require.EqualError(
		t,
		err,
		"conductor: error response too short: bodyLength=3 minimum=16",
	)
}

func TestDecodeErrorResponse_bufferOutOfBounds(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 10))

	_, err := conductor.DecodeErrorResponse(buffer, 0, 16)
	require.ErrorIs(t, err, conductor.ErrErrorResponseTooShort)
	require.EqualError(
		t,
		err,
		"conductor: error response too short: offset=0 capacity=10",
	)
}

func TestDecodeErrorResponse_negativeMessageLength(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 16))
	buffer.PutInt64(0, 7)
	buffer.PutInt32(8, 1)
	buffer.PutInt32(12, -1)

	_, err := conductor.DecodeErrorResponse(buffer, 0, 16)
	require.ErrorIs(t, err, conductor.ErrErrorResponseTooShort)
	require.EqualError(t, err, "conductor: error response too short")
}

func TestDecodeErrorResponse_lengthMismatch(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 16))
	buffer.PutInt64(0, 7)
	buffer.PutInt32(8, 1)
	buffer.PutInt32(12, 8)

	_, err := conductor.DecodeErrorResponse(buffer, 0, 16)
	require.ErrorIs(t, err, conductor.ErrErrorResponseTooShort)
	require.EqualError(
		t,
		err,
		"conductor: error response too short: bodyLength=16 required=24",
	)
}

func TestDecodeErrorResponse_bodyLongerThanDecoded(t *testing.T) {
	payload := conductor.ErrorResponseBytes(7, 1, "fail")
	buffer := atomic.Wrap(append(payload, 0xFF))

	_, err := conductor.DecodeErrorResponse(buffer, 0, len(payload)+1)
	require.ErrorIs(t, err, conductor.ErrErrorResponseTooShort)
	require.EqualError(
		t,
		err,
		"conductor: error response too short: decodedLength=20 bodyLength=21",
	)
}

func TestDecodeErrorResponse_truncatedMessage(t *testing.T) {
	payload := conductor.ErrorResponseBytes(7, 1, "fail")
	buffer := atomic.Wrap(payload)

	_, err := conductor.DecodeErrorResponse(buffer, 0, len(payload)-1)
	require.ErrorIs(t, err, conductor.ErrErrorResponseTooShort)
	require.EqualError(
		t,
		err,
		"conductor: error response too short: bodyLength=19 required=20",
	)
}

func TestEncodeErrorResponse_outOfBounds(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 16))

	err := conductor.EncodeErrorResponse(buffer, 0, conductor.ErrorResponse{
		OffendingCommandCorrelationID: 7,
		ErrorCode:                     1,
		ErrorMessage:                  "fail",
	})
	require.ErrorIs(t, err, conductor.ErrErrorResponseTooShort)
	require.EqualError(
		t,
		err,
		"conductor: error response too short: offset=0 capacity=16",
	)
}

func TestErrorResponse_roundtrip(t *testing.T) {
	want := conductor.ErrorResponse{
		OffendingCommandCorrelationID: 42,
		ErrorCode:                     99,
		ErrorMessage:                  "channel error",
	}
	buffer := atomic.Wrap(make([]byte, conductor.ErrorResponseTotalLength(len(want.ErrorMessage))))

	require.NoError(t, conductor.EncodeErrorResponse(buffer, 0, want))

	got, err := conductor.DecodeErrorResponse(
		buffer,
		0,
		conductor.ErrorResponseTotalLength(len(want.ErrorMessage)),
	)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

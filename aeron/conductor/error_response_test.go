package conductor_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestErrorResponseBytes_golden(t *testing.T) {
	require.Equal(
		t,
		[]byte{
			0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x0b, 0x00, 0x00, 0x00,
			0x04, 0x00, 0x00, 0x00,
			0x66, 0x61, 0x69, 0x6c,
		},
		conductor.ErrorResponseBytes(7, 11, "fail"),
	)
	require.Equal(
		t,
		[]byte{
			0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		},
		conductor.ErrorResponseBytes(7, 0, ""),
	)
}

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
	buffer.PutInt64(conductor.ErrorResponseOffendingCommandCorrelationIDOffset, 7)
	buffer.PutInt32(conductor.ErrorResponseErrorCodeOffset, 1)
	buffer.PutInt32(conductor.ErrorResponseErrorMessageLengthOffset, -1)

	_, err := conductor.DecodeErrorResponse(buffer, 0, 16)
	require.ErrorIs(t, err, conductor.ErrErrorResponseTooShort)
	require.EqualError(t, err, "conductor: error response too short: messageLength=-1")
}

func TestDecodeErrorResponse_lengthMismatch(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 16))
	buffer.PutInt64(conductor.ErrorResponseOffendingCommandCorrelationIDOffset, 7)
	buffer.PutInt32(conductor.ErrorResponseErrorCodeOffset, 1)
	buffer.PutInt32(conductor.ErrorResponseErrorMessageLengthOffset, 8)

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

func TestEncodeErrorResponse_nonASCII(t *testing.T) {
	const nonASCIIMessage = "fail\x80"
	buffer := atomic.Wrap(
		make([]byte, conductor.ErrorResponseTotalLength(len(nonASCIIMessage))),
	)

	err := conductor.EncodeErrorResponse(buffer, 0, conductor.ErrorResponse{
		OffendingCommandCorrelationID: 7,
		ErrorCode:                     1,
		ErrorMessage:                  nonASCIIMessage,
	})
	require.ErrorIs(t, err, atomic.ErrStringASCIINonASCII)
	require.EqualError(t, err, "atomic: string ascii non-ascii")
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

func TestErrorResponse_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		errorCode := rapid.Int32().Draw(rt, "errorCode")
		message := testutil.DrawASCIIStringLenRange(rt, "message", 0, 256)
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")

		want := conductor.ErrorResponse{
			OffendingCommandCorrelationID: correlationID,
			ErrorCode:                     errorCode,
			ErrorMessage:                  message,
		}
		bodyLength := conductor.ErrorResponseTotalLength(len(message))
		require.Len(
			rt,
			conductor.ErrorResponseBytes(correlationID, errorCode, message),
			bodyLength,
		)

		buffer := atomic.Wrap(make([]byte, offset+bodyLength))
		prefix := testutil.SnapshotBufferPrefix(buffer, offset)
		require.NoError(rt, conductor.EncodeErrorResponse(buffer, offset, want))
		require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))

		got, err := conductor.DecodeErrorResponse(buffer, offset, bodyLength)
		require.NoError(rt, err)
		require.Equal(rt, want, got)
	})
}

func TestErrorResponse_trailingPadding_rejects_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		errorCode := rapid.Int32().Draw(rt, "errorCode")
		message := testutil.DrawASCIIStringLenRange(rt, "message", 0, 256)
		trailingExtra := rapid.IntRange(1, 8).Draw(rt, "trailingExtra")

		payload := conductor.ErrorResponseBytes(correlationID, errorCode, message)
		padded := append(append([]byte(nil), payload...), make([]byte, trailingExtra)...)

		_, err := conductor.DecodeErrorResponse(atomic.Wrap(padded), 0, len(padded))
		require.ErrorIs(rt, err, conductor.ErrErrorResponseTooShort)
	})
}

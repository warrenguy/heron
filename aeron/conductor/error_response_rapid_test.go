//go:build rapid

package conductor_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestErrorResponse_roundtrip_rapid(t *testing.T) {
	t.Parallel()
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

		buffer := atomicbuffer.Wrap(make([]byte, offset+bodyLength))
		prefix := testutil.SnapshotBufferPrefix(buffer, offset)
		require.NoError(rt, conductor.EncodeErrorResponse(buffer, offset, want))
		require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))

		got, err := conductor.DecodeErrorResponse(buffer, offset, bodyLength)
		require.NoError(rt, err)
		require.Equal(rt, want, got)
	})
}

func TestErrorResponse_trailingPadding_rejects_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		correlationID := rapid.Int64().Draw(rt, "correlationID")
		errorCode := rapid.Int32().Draw(rt, "errorCode")
		message := testutil.DrawASCIIStringLenRange(rt, "message", 0, 256)
		trailingExtra := rapid.IntRange(1, 8).Draw(rt, "trailingExtra")

		payload := conductor.ErrorResponseBytes(correlationID, errorCode, message)
		padded := append(append([]byte(nil), payload...), make([]byte, trailingExtra)...)

		_, err := conductor.DecodeErrorResponse(atomicbuffer.Wrap(padded), 0, len(padded))
		require.ErrorIs(rt, err, conductor.ErrErrorResponseTooShort)
	})
}

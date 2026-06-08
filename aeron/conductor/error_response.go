package conductor

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/atomic"
)

const ErrorResponseFixedBodyLength = 16

var ErrErrorResponseTooShort = errors.New("conductor: error response too short")

type ErrorResponse struct {
	OffendingCommandCorrelationID int64
	ErrorCode                     int32
	ErrorMessage                  string
}

func ErrorResponseTotalLength(messageLen int) int {
	return ErrorResponseFixedBodyLength + messageLen
}

func DecodeErrorResponse(
	buf atomic.AtomicBuffer,
	offset int,
	length int,
) (ErrorResponse, error) {
	if err := decodeMinimumBodyLengthError(
		length,
		ErrorResponseFixedBodyLength,
		ErrErrorResponseTooShort,
	); err != nil {
		return ErrorResponse{}, err
	}
	if err := bodyBoundsError(
		buf,
		offset,
		length,
		ErrErrorResponseTooShort,
	); err != nil {
		return ErrorResponse{}, err
	}

	messageLength := buf.GetInt32(offset + 12)
	if messageLength < 0 {
		return ErrorResponse{}, ErrErrorResponseTooShort
	}

	messageLengthInt := int(messageLength)
	requiredLength := ErrorResponseFixedBodyLength + messageLengthInt
	if requiredLength > length {
		return ErrorResponse{}, fmt.Errorf(
			"%w: bodyLength=%d required=%d",
			ErrErrorResponseTooShort,
			length,
			requiredLength,
		)
	}

	decodedMessage, err := decodeTrailingStringASCII(
		buf,
		offset,
		length,
		12,
		ErrErrorResponseTooShort,
	)
	if err != nil {
		return ErrorResponse{}, err
	}
	return ErrorResponse{
		OffendingCommandCorrelationID: buf.GetInt64(offset),
		ErrorCode:                     buf.GetInt32(offset + 8),
		ErrorMessage:                  decodedMessage,
	}, nil
}

func EncodeErrorResponse(
	buf atomic.AtomicBuffer,
	offset int,
	response ErrorResponse,
) error {
	messageLength := len(response.ErrorMessage)
	requiredLength := ErrorResponseTotalLength(messageLength)
	if err := bodyBoundsError(
		buf,
		offset,
		requiredLength,
		ErrErrorResponseTooShort,
	); err != nil {
		return err
	}

	buf.PutInt64(offset, response.OffendingCommandCorrelationID)
	buf.PutInt32(offset+8, response.ErrorCode)
	_, err := atomic.PutStringASCII(buf, offset+12, response.ErrorMessage)
	return err
}

func ErrorResponseBytes(
	correlationID int64,
	errorCode int32,
	message string,
) []byte {
	return encodeToBytes(
		ErrorResponseTotalLength(len(message)),
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodeErrorResponse(buf, offset, ErrorResponse{
				OffendingCommandCorrelationID: correlationID,
				ErrorCode:                     errorCode,
				ErrorMessage:                  message,
			})
		},
	)
}

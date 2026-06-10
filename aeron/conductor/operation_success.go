package conductor

import (
	"errors"

	"github.com/warrenguy/heron/aeron/atomic"
)

var ErrOperationSuccessTooShort = errors.New("conductor: operation success too short")

type OperationSuccess struct {
	CorrelationID int64
}

// DecodeOperationSuccess decodes an ON_OPERATION_SUCCESS event body.
// The caller must ensure the broadcast body length is at least OperationSuccessBodyLength.
func DecodeOperationSuccess(
	buf atomic.AtomicBuffer,
	offset int,
) (OperationSuccess, error) {
	if err := bodyBoundsError(
		buf,
		offset,
		OperationSuccessBodyLength,
		ErrOperationSuccessTooShort,
	); err != nil {
		return OperationSuccess{}, err
	}
	return OperationSuccess{
		CorrelationID: buf.GetInt64(offset + OperationSuccessCorrelationIDOffset),
	}, nil
}

func EncodeOperationSuccess(
	buf atomic.AtomicBuffer,
	offset int,
	correlationID int64,
) error {
	if err := bodyBoundsError(
		buf,
		offset,
		OperationSuccessBodyLength,
		ErrOperationSuccessTooShort,
	); err != nil {
		return err
	}
	buf.PutInt64(offset+OperationSuccessCorrelationIDOffset, correlationID)
	return nil
}

func OperationSuccessBytes(correlationID int64) []byte {
	return encodeToBytes(
		OperationSuccessBodyLength,
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodeOperationSuccess(buf, offset, correlationID)
		},
	)
}

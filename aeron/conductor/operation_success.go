package conductor

import (
	"errors"

	"github.com/warrenguy/heron/atomicbuffer"
)

var ErrOperationSuccessTooShort = errors.New("conductor: operation success too short")

type OperationSuccess struct {
	CorrelationID int64
}

// DecodeOperationSuccess decodes an ON_OPERATION_SUCCESS event body.
//
// The caller must ensure the broadcast body length is at least OperationSuccessBodyLength.
func DecodeOperationSuccess(
	buf atomicbuffer.AtomicBuffer,
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
	buf atomicbuffer.AtomicBuffer,
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
		func(buf atomicbuffer.AtomicBuffer, offset int) error {
			return EncodeOperationSuccess(buf, offset, correlationID)
		},
	)
}

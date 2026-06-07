package conductor

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/atomic"
)

var ErrCorrelatedMessageOutOfBounds = errors.New("conductor: correlated message out of bounds")

var ErrCorrelationAndInt32OutOfBounds = errors.New(
	"conductor: correlation and int32 body out of bounds",
)

const (
	sizeOfInt64                         = 8
	CorrelatedMessageLength             = 2 * sizeOfInt64
	CorrelationAndInt32BodyLength       = sizeOfInt64 + 4
	GetNextAvailableSessionIDBodyLength = CorrelatedMessageLength + 4
)

type CorrelatedMessage struct {
	ClientID      int64
	CorrelationID int64
}

type CorrelationAndInt32 struct {
	CorrelationID int64
	Int32Value    int32
}

func bodyBoundsError(
	buffer atomic.AtomicBuffer,
	offset int,
	bodyLength int,
	wrapped error,
) error {
	capacity := buffer.Capacity()
	if offset < 0 || bodyLength < 0 || offset > capacity-bodyLength {
		return fmt.Errorf(
			"%w: offset=%d capacity=%d",
			wrapped,
			offset,
			capacity,
		)
	}
	return nil
}

func EncodeCorrelated(
	buffer atomic.AtomicBuffer,
	offset int,
	clientID int64,
	correlationID int64,
) error {
	if err := bodyBoundsError(
		buffer,
		offset,
		CorrelatedMessageLength,
		ErrCorrelatedMessageOutOfBounds,
	); err != nil {
		return err
	}
	buffer.PutInt64(offset, clientID)
	buffer.PutInt64(offset+sizeOfInt64, correlationID)
	return nil
}

func CorrelatedMessageBytes(clientID, correlationID int64) []byte {
	buffer := make([]byte, CorrelatedMessageLength)
	_ = EncodeCorrelated(atomic.Wrap(buffer), 0, clientID, correlationID)
	return buffer
}

func DecodeCorrelated(buffer atomic.AtomicBuffer, offset int) (CorrelatedMessage, error) {
	if err := bodyBoundsError(
		buffer,
		offset,
		CorrelatedMessageLength,
		ErrCorrelatedMessageOutOfBounds,
	); err != nil {
		return CorrelatedMessage{}, err
	}
	return CorrelatedMessage{
		ClientID:      buffer.GetInt64(offset),
		CorrelationID: buffer.GetInt64(offset + sizeOfInt64),
	}, nil
}

func DecodeCorrelationAndInt32(
	buffer atomic.AtomicBuffer,
	offset int,
) (CorrelationAndInt32, error) {
	if err := bodyBoundsError(
		buffer,
		offset,
		CorrelationAndInt32BodyLength,
		ErrCorrelationAndInt32OutOfBounds,
	); err != nil {
		return CorrelationAndInt32{}, err
	}
	return CorrelationAndInt32{
		CorrelationID: buffer.GetInt64(offset),
		Int32Value:    buffer.GetInt32(offset + sizeOfInt64),
	}, nil
}

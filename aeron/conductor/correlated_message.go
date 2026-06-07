package conductor

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

var (
	ErrCorrelatedMessageOutOfBounds   = errors.New("conductor: correlated message out of bounds")
	ErrCorrelatedBodyTooShort         = errors.New("conductor: correlated body length too short")
	ErrCorrelationAndInt32OutOfBounds = errors.New(
		"conductor: correlation and int32 body out of bounds",
	)
)

const (
	sizeOfInt64                         = 8
	CorrelatedMessageLength             = 2 * sizeOfInt64
	CorrelationAndInt32BodyLength       = sizeOfInt64 + 4
	GetNextAvailableSessionIDBodyLength = CorrelatedMessageLength + 4
)

func CorrelatedRingRecordLength() int {
	return record.AlignRecordLength(CorrelatedMessageLength + ringbuffer.HeaderLength)
}

type CorrelatedMessage struct {
	ClientID      int64
	CorrelationID int64
}

type CorrelationAndInt32 struct {
	CorrelationID int64
	Int32Value    int32
}

func bodyBoundsError(
	buf atomic.AtomicBuffer,
	offset, bodyLength int,
	wrapped error,
) error {
	return atomic.BoundsWrapError(buf, offset, bodyLength, wrapped)
}

func decodeMinimumBodyLengthError(
	bodyLength, minimum int,
	wrapped error,
) error {
	if bodyLength >= minimum {
		return nil
	}
	return fmt.Errorf(
		"%w: bodyLength=%d minimum=%d",
		wrapped,
		bodyLength,
		minimum,
	)
}

func putCorrelatedFields(
	buf atomic.AtomicBuffer,
	offset int,
	clientID, correlationID int64,
) {
	buf.PutInt64(offset, clientID)
	buf.PutInt64(offset+sizeOfInt64, correlationID)
}

func decodeTrailingStringASCII(
	buf atomic.AtomicBuffer,
	offset, bodyLength, stringOffset int,
	wrapped error,
) (string, error) {
	if stringOffset > bodyLength {
		return "", fmt.Errorf(
			"%w: stringOffset=%d bodyLength=%d",
			wrapped,
			stringOffset,
			bodyLength,
		)
	}
	value, encodedLength, err := atomic.GetStringASCII(
		buf.Slice(offset+stringOffset, bodyLength-stringOffset),
		0,
	)
	if err != nil {
		return "", err
	}

	decodedLength := stringOffset + encodedLength
	if decodedLength != bodyLength {
		return "", fmt.Errorf(
			"%w: decodedLength=%d bodyLength=%d",
			wrapped,
			decodedLength,
			bodyLength,
		)
	}
	return value, nil
}

func EncodeCorrelated(
	buf atomic.AtomicBuffer,
	offset int,
	clientID, correlationID int64,
) error {
	if err := bodyBoundsError(
		buf,
		offset,
		CorrelatedMessageLength,
		ErrCorrelatedMessageOutOfBounds,
	); err != nil {
		return err
	}
	putCorrelatedFields(buf, offset, clientID, correlationID)
	return nil
}

func encodeToBytes(
	length int,
	encode func(buf atomic.AtomicBuffer, offset int) error,
) []byte {
	buffer := make([]byte, length)
	if err := encode(atomic.Wrap(buffer), 0); err != nil {
		panic(err)
	}
	return buffer
}

func CorrelatedMessageBytes(clientID, correlationID int64) []byte {
	return encodeToBytes(CorrelatedMessageLength, func(buf atomic.AtomicBuffer, offset int) error {
		return EncodeCorrelated(buf, offset, clientID, correlationID)
	})
}

func DecodeCorrelated(buf atomic.AtomicBuffer, offset int) (CorrelatedMessage, error) {
	if err := bodyBoundsError(
		buf,
		offset,
		CorrelatedMessageLength,
		ErrCorrelatedMessageOutOfBounds,
	); err != nil {
		return CorrelatedMessage{}, err
	}
	return CorrelatedMessage{
		ClientID:      buf.GetInt64(offset),
		CorrelationID: buf.GetInt64(offset + sizeOfInt64),
	}, nil
}

func DecodeCorrelationAndInt32(buf atomic.AtomicBuffer, offset int) (CorrelationAndInt32, error) {
	if err := bodyBoundsError(
		buf,
		offset,
		CorrelationAndInt32BodyLength,
		ErrCorrelationAndInt32OutOfBounds,
	); err != nil {
		return CorrelationAndInt32{}, err
	}
	return CorrelationAndInt32{
		CorrelationID: buf.GetInt64(offset),
		Int32Value:    buf.GetInt32(offset + sizeOfInt64),
	}, nil
}

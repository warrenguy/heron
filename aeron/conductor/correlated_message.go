package conductor

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/record"
	"github.com/warrenguy/heron/ringbuffer"
)

var (
	ErrCorrelatedMessageOutOfBounds   = errors.New("conductor: correlated message out of bounds")
	ErrCorrelatedBodyTooShort         = errors.New("conductor: correlated body length too short")
	ErrCorrelationAndInt32OutOfBounds = errors.New(
		"conductor: correlation and int32 body out of bounds",
	)
)

const (
	CorrelatedMessageLength             = 2 * sizeOfInt64
	CorrelationAndInt32BodyLength       = sizeOfInt64 + sizeOfInt32
	NextAvailableSessionIDBodyLength    = CorrelationAndInt32BodyLength
	GetNextAvailableSessionIDBodyLength = CorrelatedMessageLength + sizeOfInt32
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
	buf atomicbuffer.AtomicBuffer,
	offset, bodyLength int,
	wrapped error,
) error {
	return atomicbuffer.BoundsWrapError(buf, offset, bodyLength, wrapped)
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
	buf atomicbuffer.AtomicBuffer,
	offset int,
	clientID, correlationID int64,
) {
	buf.PutInt64(offset+CorrelatedMessageClientIDOffset, clientID)
	buf.PutInt64(offset+CorrelatedMessageCorrelationIDOffset, correlationID)
}

func decodeTrailingStringASCII(
	buf atomicbuffer.AtomicBuffer,
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
	value, encodedLength, err := atomicbuffer.GetStringASCII(
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
	buf atomicbuffer.AtomicBuffer,
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
	encode func(buf atomicbuffer.AtomicBuffer, offset int) error,
) []byte {
	buffer := make([]byte, length)
	if err := encode(atomicbuffer.Wrap(buffer), 0); err != nil {
		panic(err)
	}
	return buffer
}

func CorrelatedMessageBytes(clientID, correlationID int64) []byte {
	return encodeToBytes(CorrelatedMessageLength, func(
		buf atomicbuffer.AtomicBuffer,
		offset int,
	) error {
		return EncodeCorrelated(buf, offset, clientID, correlationID)
	})
}

func DecodeCorrelated(buf atomicbuffer.AtomicBuffer, offset int) (CorrelatedMessage, error) {
	if err := bodyBoundsError(
		buf,
		offset,
		CorrelatedMessageLength,
		ErrCorrelatedMessageOutOfBounds,
	); err != nil {
		return CorrelatedMessage{}, err
	}
	return CorrelatedMessage{
		ClientID:      buf.GetInt64(offset + CorrelatedMessageClientIDOffset),
		CorrelationID: buf.GetInt64(offset + CorrelatedMessageCorrelationIDOffset),
	}, nil
}

func EncodeCorrelationAndInt32(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	correlationID int64,
	int32Value int32,
) error {
	if err := bodyBoundsError(
		buf,
		offset,
		CorrelationAndInt32BodyLength,
		ErrCorrelationAndInt32OutOfBounds,
	); err != nil {
		return err
	}
	buf.PutInt64(offset+CorrelationAndInt32CorrelationIDOffset, correlationID)
	buf.PutInt32(offset+CorrelationAndInt32Int32ValueOffset, int32Value)
	return nil
}

func CorrelationAndInt32Bytes(correlationID int64, int32Value int32) []byte {
	return encodeToBytes(
		CorrelationAndInt32BodyLength,
		func(buf atomicbuffer.AtomicBuffer, offset int) error {
			return EncodeCorrelationAndInt32(buf, offset, correlationID, int32Value)
		},
	)
}

func DecodeCorrelationAndInt32(
	buf atomicbuffer.AtomicBuffer,
	offset int,
) (CorrelationAndInt32, error) {
	if err := bodyBoundsError(
		buf,
		offset,
		CorrelationAndInt32BodyLength,
		ErrCorrelationAndInt32OutOfBounds,
	); err != nil {
		return CorrelationAndInt32{}, err
	}
	return CorrelationAndInt32{
		CorrelationID: buf.GetInt64(offset + CorrelationAndInt32CorrelationIDOffset),
		Int32Value:    buf.GetInt32(offset + CorrelationAndInt32Int32ValueOffset),
	}, nil
}

// DecodeNextAvailableSessionID decodes the ON_NEXT_AVAILABLE_SESSION_ID event body.
//
// The caller must ensure the broadcast body length is at least NextAvailableSessionIDBodyLength.
func DecodeNextAvailableSessionID(
	buf atomicbuffer.AtomicBuffer,
	offset int,
) (CorrelationAndInt32, error) {
	return DecodeCorrelationAndInt32(buf, offset)
}

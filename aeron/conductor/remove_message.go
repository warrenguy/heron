package conductor

import (
	"errors"

	"github.com/warrenguy/heron/aeron/atomic"
)

var ErrRemoveMessageOutOfBounds = errors.New("conductor: remove message out of bounds")

type RemoveMessage struct {
	ClientID       int64
	CorrelationID  int64
	RegistrationID int64
}

func EncodeRemoveMessage(
	buf atomic.AtomicBuffer,
	offset int,
	clientID, correlationID, registrationID int64,
) error {
	if err := bodyBoundsError(
		buf,
		offset,
		RemoveMessageBodyLength,
		ErrRemoveMessageOutOfBounds,
	); err != nil {
		return err
	}
	return encodeRemoveMessageFields(buf, offset, clientID, correlationID, registrationID)
}

func encodeRemoveMessageFields(
	buf atomic.AtomicBuffer,
	offset int,
	clientID, correlationID, registrationID int64,
) error {
	putCorrelatedFields(buf, offset, clientID, correlationID)
	buf.PutInt64(offset+RemoveMessageRegistrationIDOffset, registrationID)
	return nil
}

func DecodeRemoveMessage(
	buf atomic.AtomicBuffer,
	offset, bodyLength int,
) (RemoveMessage, error) {
	if err := decodeMinimumBodyLengthError(
		bodyLength,
		RemoveMessageBodyLength,
		ErrRemoveMessageOutOfBounds,
	); err != nil {
		return RemoveMessage{}, err
	}
	if err := bodyBoundsError(
		buf,
		offset,
		RemoveMessageBodyLength,
		ErrRemoveMessageOutOfBounds,
	); err != nil {
		return RemoveMessage{}, err
	}

	correlated, err := DecodeCorrelated(buf, offset)
	if err != nil {
		return RemoveMessage{}, err
	}

	return RemoveMessage{
		ClientID:       correlated.ClientID,
		CorrelationID:  correlated.CorrelationID,
		RegistrationID: buf.GetInt64(offset + RemoveMessageRegistrationIDOffset),
	}, nil
}

func RemoveMessageBytes(clientID, correlationID, registrationID int64) []byte {
	return encodeToBytes(RemoveMessageBodyLength, func(buf atomic.AtomicBuffer, offset int) error {
		return EncodeRemoveMessage(buf, offset, clientID, correlationID, registrationID)
	})
}

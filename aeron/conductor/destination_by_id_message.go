package conductor

import (
	"errors"

	"github.com/warrenguy/heron/atomicbuffer"
)

var ErrDestinationByIDMessageOutOfBounds = errors.New(
	"conductor: destination by id message out of bounds",
)

// DestinationByIDMessage is the to-driver body for REMOVE_DESTINATION_BY_ID.
//
// Wire layout matches DestinationByIdMessageFlyweight: correlated prefix,
// resource registration id, destination registration id.
type DestinationByIDMessage struct {
	ClientID                  int64
	CorrelationID             int64
	ResourceRegistrationID    int64
	DestinationRegistrationID int64
}

func EncodeDestinationByIDMessage(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	clientID, correlationID, resourceRegistrationID, destinationRegistrationID int64,
) error {
	if err := bodyBoundsError(
		buf,
		offset,
		DestinationByIDMessageBodyLength,
		ErrDestinationByIDMessageOutOfBounds,
	); err != nil {
		return err
	}
	putCorrelatedFields(buf, offset, clientID, correlationID)
	buf.PutInt64(offset+DestinationByIDMessageResourceRegistrationIDOffset, resourceRegistrationID)
	buf.PutInt64(
		offset+DestinationByIDMessageDestinationRegistrationIDOffset,
		destinationRegistrationID,
	)
	return nil
}

func DecodeDestinationByIDMessage(
	buf atomicbuffer.AtomicBuffer,
	offset, bodyLength int,
) (DestinationByIDMessage, error) {
	if err := decodeMinimumBodyLengthError(
		bodyLength,
		DestinationByIDMessageBodyLength,
		ErrDestinationByIDMessageOutOfBounds,
	); err != nil {
		return DestinationByIDMessage{}, err
	}
	if err := bodyBoundsError(
		buf,
		offset,
		DestinationByIDMessageBodyLength,
		ErrDestinationByIDMessageOutOfBounds,
	); err != nil {
		return DestinationByIDMessage{}, err
	}

	correlated, err := DecodeCorrelated(buf, offset)
	if err != nil {
		return DestinationByIDMessage{}, err
	}

	return DestinationByIDMessage{
		ClientID:      correlated.ClientID,
		CorrelationID: correlated.CorrelationID,
		ResourceRegistrationID: buf.GetInt64(
			offset + DestinationByIDMessageResourceRegistrationIDOffset,
		),
		DestinationRegistrationID: buf.GetInt64(
			offset + DestinationByIDMessageDestinationRegistrationIDOffset,
		),
	}, nil
}

func DestinationByIDMessageBytes(
	clientID, correlationID, resourceRegistrationID, destinationRegistrationID int64,
) []byte {
	return encodeToBytes(DestinationByIDMessageBodyLength, func(
		buf atomicbuffer.AtomicBuffer,
		offset int,
	) error {
		return EncodeDestinationByIDMessage(
			buf,
			offset,
			clientID,
			correlationID,
			resourceRegistrationID,
			destinationRegistrationID,
		)
	})
}

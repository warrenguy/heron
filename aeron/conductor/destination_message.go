package conductor

import (
	"errors"

	"github.com/warrenguy/heron/atomicbuffer"
)

var ErrDestinationMessageOutOfBounds = errors.New("conductor: destination message out of bounds")

// DestinationMessage is the to-driver body for ADD/REMOVE receive and send
// destination commands.
//
// Wire layout matches DestinationMessageFlyweight: client id and correlation
// id (correlated prefix), then registrationCorrelationId (subscription or
// publication registration id), then a length-prefixed ASCII channel string.
// The body has no stream id field.
type DestinationMessage struct {
	ClientID                  int64
	CorrelationID             int64
	RegistrationCorrelationID int64
	Channel                   string
}

func DestinationMessageBodyLength(channel string) int {
	return correlatedStreamChannelBodyLength(destinationStreamChannelLayout, channel)
}

func EncodeDestinationMessage(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	clientID, correlationID, registrationCorrelationID int64,
	channel string,
) error {
	return encodeCorrelatedStreamChannelMessage(
		buf,
		offset,
		destinationStreamChannelLayout,
		clientID,
		correlationID,
		&registrationCorrelationID,
		0,
		channel,
	)
}

func DecodeDestinationMessage(
	buf atomicbuffer.AtomicBuffer,
	offset, bodyLength int,
) (DestinationMessage, error) {
	decoded, err := decodeCorrelatedStreamChannelMessage(
		buf,
		offset,
		bodyLength,
		destinationStreamChannelLayout,
	)
	if err != nil {
		return DestinationMessage{}, err
	}

	return DestinationMessage{
		ClientID:                  decoded.Correlated.ClientID,
		CorrelationID:             decoded.Correlated.CorrelationID,
		RegistrationCorrelationID: decoded.MiddleInt64,
		Channel:                   decoded.Channel,
	}, nil
}

func DestinationMessageBytes(
	clientID, correlationID, registrationCorrelationID int64,
	channel string,
) []byte {
	return encodeToBytes(
		DestinationMessageBodyLength(channel),
		func(buf atomicbuffer.AtomicBuffer, offset int) error {
			return EncodeDestinationMessage(
				buf,
				offset,
				clientID,
				correlationID,
				registrationCorrelationID,
				channel,
			)
		},
	)
}

package conductor

import (
	"errors"

	"github.com/warrenguy/heron/agrona/atomic"
)

var ErrPublicationMessageOutOfBounds = errors.New("conductor: publication message out of bounds")

type PublicationMessage struct {
	ClientID      int64
	CorrelationID int64
	StreamID      int32
	Channel       string
}

func PublicationMessageBodyLength(channel string) int {
	return correlatedStreamChannelBodyLength(publicationStreamChannelLayout, channel)
}

func EncodePublicationMessage(
	buf atomic.AtomicBuffer,
	offset int,
	clientID, correlationID int64,
	streamID int32,
	channel string,
) error {
	return encodeCorrelatedStreamChannelMessage(
		buf,
		offset,
		publicationStreamChannelLayout,
		clientID,
		correlationID,
		nil,
		streamID,
		channel,
	)
}

func DecodePublicationMessage(
	buf atomic.AtomicBuffer,
	offset, bodyLength int,
) (PublicationMessage, error) {
	decoded, err := decodeCorrelatedStreamChannelMessage(
		buf,
		offset,
		bodyLength,
		publicationStreamChannelLayout,
	)
	if err != nil {
		return PublicationMessage{}, err
	}

	return PublicationMessage{
		ClientID:      decoded.Correlated.ClientID,
		CorrelationID: decoded.Correlated.CorrelationID,
		StreamID:      decoded.StreamID,
		Channel:       decoded.Channel,
	}, nil
}

func PublicationMessageBytes(
	clientID, correlationID int64,
	streamID int32,
	channel string,
) []byte {
	return encodeToBytes(
		PublicationMessageBodyLength(channel),
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodePublicationMessage(buf, offset, clientID, correlationID, streamID, channel)
		},
	)
}

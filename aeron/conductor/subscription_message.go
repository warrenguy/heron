package conductor

import (
	"errors"

	"github.com/warrenguy/heron/aeron/atomic"
)

var ErrSubscriptionMessageOutOfBounds = errors.New("conductor: subscription message out of bounds")

const NullRegistrationCorrelationID = int64(-1)

type SubscriptionMessage struct {
	ClientID                  int64
	CorrelationID             int64
	RegistrationCorrelationID int64
	StreamID                  int32
	Channel                   string
}

func SubscriptionMessageBodyLength(channel string) int {
	return correlatedStreamChannelBodyLength(subscriptionStreamChannelLayout, channel)
}

func EncodeSubscriptionMessage(
	buf atomic.AtomicBuffer,
	offset int,
	clientID, correlationID, registrationCorrelationID int64,
	streamID int32,
	channel string,
) error {
	return encodeCorrelatedStreamChannelMessage(
		buf,
		offset,
		subscriptionStreamChannelLayout,
		clientID,
		correlationID,
		&registrationCorrelationID,
		streamID,
		channel,
	)
}

func DecodeSubscriptionMessage(
	buf atomic.AtomicBuffer,
	offset, bodyLength int,
) (SubscriptionMessage, error) {
	decoded, err := decodeCorrelatedStreamChannelMessage(
		buf,
		offset,
		bodyLength,
		subscriptionStreamChannelLayout,
	)
	if err != nil {
		return SubscriptionMessage{}, err
	}

	return SubscriptionMessage{
		ClientID:                  decoded.Correlated.ClientID,
		CorrelationID:             decoded.Correlated.CorrelationID,
		RegistrationCorrelationID: decoded.MiddleInt64,
		StreamID:                  decoded.StreamID,
		Channel:                   decoded.Channel,
	}, nil
}

func SubscriptionMessageBytes(
	clientID, correlationID, registrationCorrelationID int64,
	streamID int32,
	channel string,
) []byte {
	return encodeToBytes(
		SubscriptionMessageBodyLength(channel),
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodeSubscriptionMessage(
				buf,
				offset,
				clientID,
				correlationID,
				registrationCorrelationID,
				streamID,
				channel,
			)
		},
	)
}

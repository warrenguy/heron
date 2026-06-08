package conductor

import (
	"github.com/warrenguy/heron/aeron/atomic"
)

const UnavailableImageFixedLength = 20

type UnavailableImage struct {
	CorrelationID              int64
	SubscriptionRegistrationID int64
	StreamID                   int32
	Channel                    string
}

func UnavailableImageBodyLength(channel string) int {
	return fixedPlusTrailingStringBodyLength(UnavailableImageFixedLength, channel)
}

func DecodeUnavailableImage(
	buf atomic.AtomicBuffer,
	offset int,
	capacity int,
) (UnavailableImage, error) {
	if err := decodeMinimumBodyLengthError(
		capacity,
		UnavailableImageFixedLength+atomic.StringASCIIHeaderLength,
		ErrEventBodyTooShort,
	); err != nil {
		return UnavailableImage{}, err
	}
	if err := bodyBoundsError(buf, offset, capacity, ErrEventBodyTooShort); err != nil {
		return UnavailableImage{}, err
	}

	channel, _, err := decodeStringASCIIInBody(
		buf,
		offset,
		capacity,
		UnavailableImageFixedLength,
		ErrEventBodyTooShort,
	)
	if err != nil {
		return UnavailableImage{}, err
	}

	return UnavailableImage{
		CorrelationID:              buf.GetInt64(offset),
		SubscriptionRegistrationID: buf.GetInt64(offset + 8),
		StreamID:                   buf.GetInt32(offset + 16),
		Channel:                    channel,
	}, nil
}

func EncodeUnavailableImage(
	buf atomic.AtomicBuffer,
	offset int,
	event UnavailableImage,
) error {
	bodyLength := UnavailableImageBodyLength(event.Channel)
	if err := bodyBoundsError(buf, offset, bodyLength, ErrEventBodyTooShort); err != nil {
		return err
	}

	buf.PutInt64(offset, event.CorrelationID)
	buf.PutInt64(offset+8, event.SubscriptionRegistrationID)
	buf.PutInt32(offset+16, event.StreamID)
	_, err := encodeStringASCIIInBody(buf, offset, UnavailableImageFixedLength, event.Channel)
	return err
}

func UnavailableImageBytes(event UnavailableImage) []byte {
	return encodeToBytes(
		UnavailableImageBodyLength(event.Channel),
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodeUnavailableImage(buf, offset, event)
		},
	)
}

package conductor

import "github.com/warrenguy/heron/atomicbuffer"

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
	buf atomicbuffer.AtomicBuffer,
	offset int,
	capacity int,
) (UnavailableImage, error) {
	if err := decodeMinimumBodyLengthError(
		capacity,
		UnavailableImageFixedLength+atomicbuffer.StringASCIIHeaderLength,
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
		CorrelationID: buf.GetInt64(offset + UnavailableImageCorrelationIDOffset),
		SubscriptionRegistrationID: buf.GetInt64(
			offset + UnavailableImageSubscriptionRegistrationIDOffset,
		),
		StreamID: buf.GetInt32(offset + UnavailableImageStreamIDOffset),
		Channel:  channel,
	}, nil
}

func EncodeUnavailableImage(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	event UnavailableImage,
) error {
	bodyLength := UnavailableImageBodyLength(event.Channel)
	if err := bodyBoundsError(buf, offset, bodyLength, ErrEventBodyTooShort); err != nil {
		return err
	}

	buf.PutInt64(offset+UnavailableImageCorrelationIDOffset, event.CorrelationID)
	buf.PutInt64(
		offset+UnavailableImageSubscriptionRegistrationIDOffset,
		event.SubscriptionRegistrationID,
	)
	buf.PutInt32(offset+UnavailableImageStreamIDOffset, event.StreamID)
	_, err := encodeStringASCIIInBody(buf, offset, UnavailableImageFixedLength, event.Channel)
	return err
}

func UnavailableImageBytes(event UnavailableImage) []byte {
	return encodeToBytes(
		UnavailableImageBodyLength(event.Channel),
		func(buf atomicbuffer.AtomicBuffer, offset int) error {
			return EncodeUnavailableImage(buf, offset, event)
		},
	)
}

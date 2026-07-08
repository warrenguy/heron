package conductor

import "github.com/warrenguy/heron/atomicbuffer"

type SubscriptionReady struct {
	CorrelationID          int64
	ChannelStatusCounterID int32
}

func DecodeSubscriptionReady(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	capacity int,
) (SubscriptionReady, error) {
	if err := decodeMinimumBodyLengthError(
		capacity,
		SubscriptionReadyBodyLength,
		ErrEventBodyTooShort,
	); err != nil {
		return SubscriptionReady{}, err
	}
	if err := bodyBoundsError(buf, offset, capacity, ErrEventBodyTooShort); err != nil {
		return SubscriptionReady{}, err
	}

	return SubscriptionReady{
		CorrelationID:          buf.GetInt64(offset + SubscriptionReadyCorrelationIDOffset),
		ChannelStatusCounterID: buf.GetInt32(offset + SubscriptionReadyChannelStatusCounterIDOffset),
	}, nil
}

func EncodeSubscriptionReady(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	event SubscriptionReady,
) error {
	if err := bodyBoundsError(
		buf,
		offset,
		SubscriptionReadyBodyLength,
		ErrEventBodyTooShort,
	); err != nil {
		return err
	}

	buf.PutInt64(offset+SubscriptionReadyCorrelationIDOffset, event.CorrelationID)
	buf.PutInt32(offset+SubscriptionReadyChannelStatusCounterIDOffset, event.ChannelStatusCounterID)
	return nil
}

func SubscriptionReadyBytes(event SubscriptionReady) []byte {
	return encodeToBytes(
		SubscriptionReadyBodyLength,
		func(buf atomicbuffer.AtomicBuffer, offset int) error {
			return EncodeSubscriptionReady(buf, offset, event)
		},
	)
}

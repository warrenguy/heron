package conductor

import (
	"github.com/warrenguy/heron/aeron/atomic"
)

const SubscriptionReadyBodyLength = 12

type SubscriptionReady struct {
	CorrelationID          int64
	ChannelStatusCounterID int32
}

func DecodeSubscriptionReady(
	buf atomic.AtomicBuffer,
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
		CorrelationID:          buf.GetInt64(offset),
		ChannelStatusCounterID: buf.GetInt32(offset + 8),
	}, nil
}

func EncodeSubscriptionReady(
	buf atomic.AtomicBuffer,
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

	buf.PutInt64(offset, event.CorrelationID)
	buf.PutInt32(offset+8, event.ChannelStatusCounterID)
	return nil
}

func SubscriptionReadyBytes(event SubscriptionReady) []byte {
	return encodeToBytes(
		SubscriptionReadyBodyLength,
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodeSubscriptionReady(buf, offset, event)
		},
	)
}

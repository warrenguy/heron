package conductor

import "github.com/warrenguy/heron/atomicbuffer"

type correlatedStreamChannelLayout struct {
	middleInt64Offset int
	streamIDOffset    int
	channelOffset     int
	minimumBodyLength int
	errOutOfBounds    error
}

type correlatedStreamChannelDecoded struct {
	Correlated  CorrelatedMessage
	MiddleInt64 int64
	StreamID    int32
	Channel     string
}

var (
	publicationStreamChannelLayout = correlatedStreamChannelLayout{
		middleInt64Offset: -1,
		streamIDOffset:    PublicationMessageStreamIDOffset,
		channelOffset:     PublicationMessageChannelOffset,
		minimumBodyLength: PublicationMessageMinimumBodyLength,
		errOutOfBounds:    ErrPublicationMessageOutOfBounds,
	}
	subscriptionStreamChannelLayout = correlatedStreamChannelLayout{
		middleInt64Offset: SubscriptionMessageRegistrationCorrelationIDOffset,
		streamIDOffset:    SubscriptionMessageStreamIDOffset,
		channelOffset:     SubscriptionMessageChannelOffset,
		minimumBodyLength: SubscriptionMessageMinimumBodyLength,
		errOutOfBounds:    ErrSubscriptionMessageOutOfBounds,
	}
)

func correlatedStreamChannelBodyLength(
	layout correlatedStreamChannelLayout,
	channel string,
) int {
	return layout.channelOffset + atomicbuffer.StringASCIIEncodedLength(len(channel))
}

// Bounds failures return before any write.
//
// Other encode failures may leave fixed-prefix fields written; DriverProxy aborts
// uncommitted ring claims; standalone callers must not use the buffer.
func encodeCorrelatedStreamChannelMessage(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	layout correlatedStreamChannelLayout,
	clientID, correlationID int64,
	middleInt64 *int64,
	streamID int32,
	channel string,
) error {
	bodyLength := correlatedStreamChannelBodyLength(layout, channel)
	if err := bodyBoundsError(
		buf,
		offset,
		bodyLength,
		layout.errOutOfBounds,
	); err != nil {
		return err
	}
	if err := EncodeCorrelated(buf, offset, clientID, correlationID); err != nil {
		return err
	}
	if layout.middleInt64Offset >= 0 {
		if middleInt64 == nil {
			panic("conductor: middle int64 required for stream channel layout")
		}
		buf.PutInt64(offset+layout.middleInt64Offset, *middleInt64)
	}
	buf.PutInt32(offset+layout.streamIDOffset, streamID)
	_, err := atomicbuffer.PutStringASCII(buf, offset+layout.channelOffset, channel)
	return err
}

func decodeCorrelatedStreamChannelMessage(
	buf atomicbuffer.AtomicBuffer,
	offset, bodyLength int,
	layout correlatedStreamChannelLayout,
) (correlatedStreamChannelDecoded, error) {
	if err := decodeMinimumBodyLengthError(
		bodyLength,
		layout.minimumBodyLength,
		layout.errOutOfBounds,
	); err != nil {
		return correlatedStreamChannelDecoded{}, err
	}
	if err := bodyBoundsError(
		buf,
		offset,
		bodyLength,
		layout.errOutOfBounds,
	); err != nil {
		return correlatedStreamChannelDecoded{}, err
	}

	correlated, err := DecodeCorrelated(buf, offset)
	if err != nil {
		return correlatedStreamChannelDecoded{}, err
	}

	channel, err := decodeTrailingStringASCII(
		buf,
		offset,
		bodyLength,
		layout.channelOffset,
		layout.errOutOfBounds,
	)
	if err != nil {
		return correlatedStreamChannelDecoded{}, err
	}

	decoded := correlatedStreamChannelDecoded{
		Correlated: correlated,
		StreamID:   buf.GetInt32(offset + layout.streamIDOffset),
		Channel:    channel,
	}
	if layout.middleInt64Offset >= 0 {
		decoded.MiddleInt64 = buf.GetInt64(offset + layout.middleInt64Offset)
	}

	return decoded, nil
}

package conductor

import (
	"github.com/warrenguy/heron/agrona/atomic"
)

// LogFileName is decoded now; caller must mmap before Offer succeeds.
type PublicationBuffersReady struct {
	CorrelationID             int64
	RegistrationID            int64
	SessionID                 int32
	StreamID                  int32
	PublicationLimitCounterID int32
	ChannelStatusCounterID    int32
	LogFileName               string
}

func PublicationBuffersReadyBodyLength(logFileName string) int {
	return fixedPlusTrailingStringBodyLength(PublicationBuffersReadyFixedLength, logFileName)
}

func DecodePublicationBuffersReady(
	buf atomic.AtomicBuffer,
	offset int,
	capacity int,
) (PublicationBuffersReady, error) {
	if err := decodeMinimumBodyLengthError(
		capacity,
		PublicationBuffersReadyFixedLength+atomic.StringASCIIHeaderLength,
		ErrEventBodyTooShort,
	); err != nil {
		return PublicationBuffersReady{}, err
	}
	if err := bodyBoundsError(buf, offset, capacity, ErrEventBodyTooShort); err != nil {
		return PublicationBuffersReady{}, err
	}

	logFileName, _, err := decodeStringASCIIInBody(
		buf,
		offset,
		capacity,
		PublicationBuffersReadyFixedLength,
		ErrEventBodyTooShort,
	)
	if err != nil {
		return PublicationBuffersReady{}, err
	}

	return PublicationBuffersReady{
		CorrelationID:  buf.GetInt64(offset + PublicationBuffersReadyCorrelationIDOffset),
		RegistrationID: buf.GetInt64(offset + PublicationBuffersReadyRegistrationIDOffset),
		SessionID:      buf.GetInt32(offset + PublicationBuffersReadySessionIDOffset),
		StreamID:       buf.GetInt32(offset + PublicationBuffersReadyStreamIDOffset),
		PublicationLimitCounterID: buf.GetInt32(
			offset + PublicationBuffersReadyPublicationLimitCounterIDOffset,
		),
		ChannelStatusCounterID: buf.GetInt32(
			offset + PublicationBuffersReadyChannelStatusCounterIDOffset,
		),
		LogFileName: logFileName,
	}, nil
}

func EncodePublicationBuffersReady(
	buf atomic.AtomicBuffer,
	offset int,
	event PublicationBuffersReady,
) error {
	bodyLength := PublicationBuffersReadyBodyLength(event.LogFileName)
	if err := bodyBoundsError(buf, offset, bodyLength, ErrEventBodyTooShort); err != nil {
		return err
	}

	buf.PutInt64(offset+PublicationBuffersReadyCorrelationIDOffset, event.CorrelationID)
	buf.PutInt64(offset+PublicationBuffersReadyRegistrationIDOffset, event.RegistrationID)
	buf.PutInt32(offset+PublicationBuffersReadySessionIDOffset, event.SessionID)
	buf.PutInt32(offset+PublicationBuffersReadyStreamIDOffset, event.StreamID)
	buf.PutInt32(
		offset+PublicationBuffersReadyPublicationLimitCounterIDOffset,
		event.PublicationLimitCounterID,
	)
	buf.PutInt32(
		offset+PublicationBuffersReadyChannelStatusCounterIDOffset,
		event.ChannelStatusCounterID,
	)
	_, err := encodeStringASCIIInBody(
		buf,
		offset,
		PublicationBuffersReadyFixedLength,
		event.LogFileName,
	)
	return err
}

func PublicationBuffersReadyBytes(event PublicationBuffersReady) []byte {
	return encodeToBytes(
		PublicationBuffersReadyBodyLength(event.LogFileName),
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodePublicationBuffersReady(buf, offset, event)
		},
	)
}

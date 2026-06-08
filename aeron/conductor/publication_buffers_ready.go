package conductor

import (
	"github.com/warrenguy/heron/aeron/atomic"
)

const PublicationBuffersReadyFixedLength = 32

// LogFileName is decoded now; step 4/5 must mmap before Offer succeeds.
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
		CorrelationID:             buf.GetInt64(offset),
		RegistrationID:            buf.GetInt64(offset + 8),
		SessionID:                 buf.GetInt32(offset + 16),
		StreamID:                  buf.GetInt32(offset + 20),
		PublicationLimitCounterID: buf.GetInt32(offset + 24),
		ChannelStatusCounterID:    buf.GetInt32(offset + 28),
		LogFileName:               logFileName,
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

	buf.PutInt64(offset, event.CorrelationID)
	buf.PutInt64(offset+8, event.RegistrationID)
	buf.PutInt32(offset+16, event.SessionID)
	buf.PutInt32(offset+20, event.StreamID)
	buf.PutInt32(offset+24, event.PublicationLimitCounterID)
	buf.PutInt32(offset+28, event.ChannelStatusCounterID)
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

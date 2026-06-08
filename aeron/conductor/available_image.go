package conductor

import (
	"fmt"

	"github.com/warrenguy/heron/aeron/atomic"
)

const AvailableImageFixedLength = 28

type AvailableImage struct {
	CorrelationID              int64
	SessionID                  int32
	StreamID                   int32
	SubscriptionRegistrationID int64
	SubscriberPositionID       int32
	LogFileName                string
	SourceIdentity             string
}

func availableImageSourceIdentityOffset(logFileNameFieldEnd int) int {
	return atomic.Align(logFileNameFieldEnd, 4)
}

func SourceIdentityOffset(
	buf atomic.AtomicBuffer,
	bodyOffset, bodyLength, logFileNameFieldOffset int,
) (int, error) {
	if err := bodyBoundsError(buf, bodyOffset, bodyLength, ErrEventBodyTooShort); err != nil {
		return 0, err
	}

	_, logFileNameEnd, err := decodeStringASCIIInBody(
		buf,
		bodyOffset,
		bodyLength,
		logFileNameFieldOffset,
		ErrEventBodyTooShort,
	)
	if err != nil {
		return 0, err
	}

	aligned := availableImageSourceIdentityOffset(logFileNameEnd)
	if aligned+atomic.StringASCIIHeaderLength > bodyLength {
		return 0, fmt.Errorf(
			"%w: need=%d got=%d",
			ErrEventBodyTooShort,
			aligned+atomic.StringASCIIHeaderLength,
			bodyLength,
		)
	}
	return aligned, nil
}

func AvailableImageBodyLength(logFileName, sourceIdentity string) int {
	logFileNameFieldEnd := AvailableImageFixedLength +
		atomic.StringASCIIEncodedLength(len(logFileName))
	sourceIdentityOffset := availableImageSourceIdentityOffset(logFileNameFieldEnd)
	return sourceIdentityOffset + atomic.StringASCIIEncodedLength(len(sourceIdentity))
}

func DecodeAvailableImage(
	buf atomic.AtomicBuffer,
	offset int,
	capacity int,
) (AvailableImage, error) {
	if err := decodeMinimumBodyLengthError(
		capacity,
		AvailableImageFixedLength+atomic.StringASCIIHeaderLength,
		ErrEventBodyTooShort,
	); err != nil {
		return AvailableImage{}, err
	}
	if err := bodyBoundsError(buf, offset, capacity, ErrEventBodyTooShort); err != nil {
		return AvailableImage{}, err
	}

	logFileName, logFileNameEnd, err := decodeStringASCIIInBody(
		buf,
		offset,
		capacity,
		AvailableImageFixedLength,
		ErrEventBodyTooShort,
	)
	if err != nil {
		return AvailableImage{}, err
	}

	sourceIdentityOffset := availableImageSourceIdentityOffset(logFileNameEnd)
	sourceIdentity, _, err := decodeStringASCIIInBody(
		buf,
		offset,
		capacity,
		sourceIdentityOffset,
		ErrEventBodyTooShort,
	)
	if err != nil {
		return AvailableImage{}, err
	}

	return AvailableImage{
		CorrelationID:              buf.GetInt64(offset),
		SessionID:                  buf.GetInt32(offset + 8),
		StreamID:                   buf.GetInt32(offset + 12),
		SubscriptionRegistrationID: buf.GetInt64(offset + 16),
		SubscriberPositionID:       buf.GetInt32(offset + 24),
		LogFileName:                logFileName,
		SourceIdentity:             sourceIdentity,
	}, nil
}

func EncodeAvailableImage(
	buf atomic.AtomicBuffer,
	offset int,
	event AvailableImage,
) error {
	bodyLength := AvailableImageBodyLength(event.LogFileName, event.SourceIdentity)
	if err := bodyBoundsError(buf, offset, bodyLength, ErrEventBodyTooShort); err != nil {
		return err
	}

	buf.PutInt64(offset, event.CorrelationID)
	buf.PutInt32(offset+8, event.SessionID)
	buf.PutInt32(offset+12, event.StreamID)
	buf.PutInt64(offset+16, event.SubscriptionRegistrationID)
	buf.PutInt32(offset+24, event.SubscriberPositionID)

	logFileNameEnd, err := encodeStringASCIIInBody(
		buf,
		offset,
		AvailableImageFixedLength,
		event.LogFileName,
	)
	if err != nil {
		return err
	}

	sourceIdentityOffset := availableImageSourceIdentityOffset(logFileNameEnd)
	if sourceIdentityOffset > bodyLength {
		return ErrEventBodyTooShort
	}
	_, err = encodeStringASCIIInBody(buf, offset, sourceIdentityOffset, event.SourceIdentity)
	return err
}

func AvailableImageBytes(event AvailableImage) []byte {
	return encodeToBytes(
		AvailableImageBodyLength(event.LogFileName, event.SourceIdentity),
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodeAvailableImage(buf, offset, event)
		},
	)
}

package conductor

import (
	"encoding/binary"

	"github.com/warrenguy/heron/aeron/atomic"
)

const PublicationErrorFixedLength = 64

type PublicationError struct {
	RegistrationID            int64
	DestinationRegistrationID int64
	SessionID                 int32
	StreamID                  int32
	ReceiverID                int64
	GroupTag                  int64
	AddressType               int16
	AddressPort               uint16
	Address                   [16]byte
	ErrorCode                 int32
	ErrorMessage              string
}

func PublicationErrorBodyLength(errorMessage string) int {
	return fixedPlusTrailingStringBodyLength(PublicationErrorFixedLength, errorMessage)
}

func DecodePublicationError(
	buf atomic.AtomicBuffer,
	offset int,
	capacity int,
) (PublicationError, error) {
	if err := decodeMinimumBodyLengthError(
		capacity,
		PublicationErrorFixedLength+atomic.StringASCIIHeaderLength,
		ErrEventBodyTooShort,
	); err != nil {
		return PublicationError{}, err
	}
	if err := bodyBoundsError(buf, offset, capacity, ErrEventBodyTooShort); err != nil {
		return PublicationError{}, err
	}

	errorMessage, _, err := decodeStringASCIIInBody(
		buf,
		offset,
		capacity,
		PublicationErrorFixedLength,
		ErrEventBodyTooShort,
	)
	if err != nil {
		return PublicationError{}, err
	}

	var address [16]byte
	copy(address[:], buf.Slice(offset+44, 16).Bytes())

	return PublicationError{
		RegistrationID:            buf.GetInt64(offset),
		DestinationRegistrationID: buf.GetInt64(offset + 8),
		SessionID:                 buf.GetInt32(offset + 16),
		StreamID:                  buf.GetInt32(offset + 20),
		ReceiverID:                buf.GetInt64(offset + 24),
		GroupTag:                  buf.GetInt64(offset + 32),
		AddressType: int16(
			binary.LittleEndian.Uint16(buf.Slice(offset+40, 2).Bytes()),
		),
		AddressPort:  binary.LittleEndian.Uint16(buf.Slice(offset+42, 2).Bytes()),
		Address:      address,
		ErrorCode:    buf.GetInt32(offset + 60),
		ErrorMessage: errorMessage,
	}, nil
}

func EncodePublicationError(
	buf atomic.AtomicBuffer,
	offset int,
	event PublicationError,
) error {
	bodyLength := PublicationErrorBodyLength(event.ErrorMessage)
	if err := bodyBoundsError(buf, offset, bodyLength, ErrEventBodyTooShort); err != nil {
		return err
	}

	buf.PutInt64(offset, event.RegistrationID)
	buf.PutInt64(offset+8, event.DestinationRegistrationID)
	buf.PutInt32(offset+16, event.SessionID)
	buf.PutInt32(offset+20, event.StreamID)
	buf.PutInt64(offset+24, event.ReceiverID)
	buf.PutInt64(offset+32, event.GroupTag)
	binary.LittleEndian.PutUint16(
		buf.Slice(offset+40, 2).Bytes(),
		uint16(event.AddressType),
	)
	binary.LittleEndian.PutUint16(buf.Slice(offset+42, 2).Bytes(), event.AddressPort)
	buf.PutBytes(offset+44, event.Address[:])
	buf.PutInt32(offset+60, event.ErrorCode)
	_, err := encodeStringASCIIInBody(
		buf,
		offset,
		PublicationErrorFixedLength,
		event.ErrorMessage,
	)
	return err
}

func PublicationErrorBytes(event PublicationError) []byte {
	return encodeToBytes(
		PublicationErrorBodyLength(event.ErrorMessage),
		func(buf atomic.AtomicBuffer, offset int) error {
			return EncodePublicationError(buf, offset, event)
		},
	)
}

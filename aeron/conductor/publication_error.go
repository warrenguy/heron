package conductor

import (
	"encoding/binary"
	"net"

	"github.com/warrenguy/heron/atomicbuffer"
)

type PublicationError struct {
	RegistrationID            int64
	DestinationRegistrationID int64
	SessionID                 int32
	StreamID                  int32
	ReceiverID                int64
	GroupTag                  int64
	AddressType               int16
	AddressPort               uint16
	Address                   [net.IPv6len]byte
	ErrorCode                 int32
	ErrorMessage              string
}

func PublicationErrorBodyLength(errorMessage string) int {
	return fixedPlusTrailingStringBodyLength(PublicationErrorFixedLength, errorMessage)
}

func DecodePublicationError(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	capacity int,
) (PublicationError, error) {
	if err := decodeMinimumBodyLengthError(
		capacity,
		PublicationErrorFixedLength+atomicbuffer.StringASCIIHeaderLength,
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

	var address [net.IPv6len]byte
	copy(address[:], buf.Slice(offset+PublicationErrorAddressOffset, net.IPv6len).Bytes())

	return PublicationError{
		RegistrationID:            buf.GetInt64(offset + PublicationErrorRegistrationIDOffset),
		DestinationRegistrationID: buf.GetInt64(offset + PublicationErrorDestinationRegistrationIDOffset),
		SessionID:                 buf.GetInt32(offset + PublicationErrorSessionIDOffset),
		StreamID:                  buf.GetInt32(offset + PublicationErrorStreamIDOffset),
		ReceiverID:                buf.GetInt64(offset + PublicationErrorReceiverIDOffset),
		GroupTag:                  buf.GetInt64(offset + PublicationErrorGroupTagOffset),
		//nolint:gosec // G115: the address type is a signed int16 on the wire
		AddressType: int16(
			binary.LittleEndian.Uint16(
				buf.Slice(offset+PublicationErrorAddressTypeOffset, sizeOfUint16).Bytes(),
			),
		),
		AddressPort: binary.LittleEndian.Uint16(
			buf.Slice(offset+PublicationErrorAddressPortOffset, sizeOfUint16).Bytes(),
		),
		Address:      address,
		ErrorCode:    buf.GetInt32(offset + PublicationErrorErrorCodeOffset),
		ErrorMessage: errorMessage,
	}, nil
}

func EncodePublicationError(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	event PublicationError,
) error {
	bodyLength := PublicationErrorBodyLength(event.ErrorMessage)
	if err := bodyBoundsError(buf, offset, bodyLength, ErrEventBodyTooShort); err != nil {
		return err
	}

	buf.PutInt64(offset+PublicationErrorRegistrationIDOffset, event.RegistrationID)
	buf.PutInt64(
		offset+PublicationErrorDestinationRegistrationIDOffset,
		event.DestinationRegistrationID,
	)
	buf.PutInt32(offset+PublicationErrorSessionIDOffset, event.SessionID)
	buf.PutInt32(offset+PublicationErrorStreamIDOffset, event.StreamID)
	buf.PutInt64(offset+PublicationErrorReceiverIDOffset, event.ReceiverID)
	buf.PutInt64(offset+PublicationErrorGroupTagOffset, event.GroupTag)
	binary.LittleEndian.PutUint16(
		buf.Slice(offset+PublicationErrorAddressTypeOffset, sizeOfUint16).Bytes(),
		//nolint:gosec // G115: the address type is a signed int16 on the wire
		uint16(event.AddressType),
	)
	binary.LittleEndian.PutUint16(
		buf.Slice(offset+PublicationErrorAddressPortOffset, sizeOfUint16).Bytes(),
		event.AddressPort,
	)
	buf.PutBytes(offset+PublicationErrorAddressOffset, event.Address[:])
	buf.PutInt32(offset+PublicationErrorErrorCodeOffset, event.ErrorCode)
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
		func(buf atomicbuffer.AtomicBuffer, offset int) error {
			return EncodePublicationError(buf, offset, event)
		},
	)
}

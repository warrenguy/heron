package conductor

import "github.com/warrenguy/heron/atomicbuffer"

const RemovePublicationFlagRevoke = int64(0x1)

type RemovePublicationMessage struct {
	RemoveMessage
	Revoke bool
}

func EncodeRemovePublicationMessage(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	clientID, correlationID, registrationID int64,
	revoke bool,
) error {
	if err := bodyBoundsError(
		buf,
		offset,
		RemovePublicationBodyLength,
		ErrRemoveMessageOutOfBounds,
	); err != nil {
		return err
	}
	if err := encodeRemoveMessageFields(
		buf,
		offset,
		clientID,
		correlationID,
		registrationID,
	); err != nil {
		return err
	}
	var flags int64
	if revoke {
		flags = RemovePublicationFlagRevoke
	}
	buf.PutInt64(offset+RemovePublicationFlagsOffset, flags)
	return nil
}

// DecodeRemovePublicationMessage decodes REMOVE_PUBLICATION at offset.
//
// Legacy 24-byte bodies omit the revoke flags field; Revoke is false.
//
// Matches Aeron Java revoke(messageLength).
func DecodeRemovePublicationMessage(
	buf atomicbuffer.AtomicBuffer,
	offset, bodyLength int,
) (RemovePublicationMessage, error) {
	removeMessage, err := DecodeRemoveMessage(buf, offset, bodyLength)
	if err != nil {
		return RemovePublicationMessage{}, err
	}

	revoke := false
	if bodyLength >= RemovePublicationBodyLength {
		if err := bodyBoundsError(
			buf,
			offset,
			RemovePublicationBodyLength,
			ErrRemoveMessageOutOfBounds,
		); err != nil {
			return RemovePublicationMessage{}, err
		}
		revoke = buf.GetInt64(offset+RemovePublicationFlagsOffset)&RemovePublicationFlagRevoke != 0
	}

	return RemovePublicationMessage{
		RemoveMessage: removeMessage,
		Revoke:        revoke,
	}, nil
}

func RemovePublicationMessageBytes(
	clientID, correlationID, registrationID int64,
	revoke bool,
) []byte {
	return encodeToBytes(RemovePublicationBodyLength, func(
		buf atomicbuffer.AtomicBuffer,
		offset int,
	) error {
		return EncodeRemovePublicationMessage(
			buf,
			offset,
			clientID,
			correlationID,
			registrationID,
			revoke,
		)
	})
}

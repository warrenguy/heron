package atomic

import (
	"errors"
	"fmt"
	"math"
)

var (
	ErrStringASCIIOutOfBounds   = errors.New("atomic: string ascii out of bounds")
	ErrStringASCIILengthInvalid = errors.New("atomic: string ascii length invalid")
	ErrStringASCIINonASCII      = errors.New("atomic: string ascii non-ascii")
)

const StringASCIIHeaderLength = SizeInt32

// StringASCIIEncodedLength is the Agrona putStringAscii wire size: 4-byte
// little-endian length plus valueByteLen raw payload bytes.
func StringASCIIEncodedLength(valueByteLen int) int {
	return StringASCIIHeaderLength + valueByteLen
}

func validateStringASCIIValueByteLen(valueByteLen int) error {
	if valueByteLen > math.MaxInt32 {
		return ErrStringASCIILengthInvalid
	}
	return nil
}

func validateStringASCIIValue(value string) error {
	for i := 0; i < len(value); i++ {
		if value[i] > 127 {
			return ErrStringASCIINonASCII
		}
	}
	return nil
}

// PutStringASCII encodes Agrona putStringAscii layout (LE int32 length, then bytes).
// Intended for ASCII wire fields such as Aeron channel URIs.
// Rejects non-ASCII input; Agrona Java sanitizes high chars to '?'.
func PutStringASCII(buf AtomicBuffer, offset int, value string) (int, error) {
	if err := validateStringASCIIValue(value); err != nil {
		return 0, err
	}

	valueByteLen := len(value)
	if err := validateStringASCIIValueByteLen(valueByteLen); err != nil {
		return 0, err
	}

	encodedLength := StringASCIIEncodedLength(valueByteLen)
	if err := BoundsWrapError(buf, offset, encodedLength, ErrStringASCIIOutOfBounds); err != nil {
		return 0, err
	}

	buf.PutInt32(offset, int32(valueByteLen))
	buf.PutStringBytes(offset+StringASCIIHeaderLength, value)
	return encodedLength, nil
}

// GetStringASCII decodes Agrona getStringAscii layout at offset.
// Returns raw payload bytes as a Go string; no ASCII validation on decode.
func GetStringASCII(buf AtomicBuffer, offset int) (string, int, error) {
	if err := BoundsWrapError(
		buf,
		offset,
		StringASCIIHeaderLength,
		ErrStringASCIIOutOfBounds,
	); err != nil {
		return "", 0, err
	}

	strLen := buf.GetInt32(offset)
	if strLen < 0 {
		return "", 0, ErrStringASCIILengthInvalid
	}

	strLenInt := int(strLen)
	encodedLength := StringASCIIEncodedLength(strLenInt)
	if strLenInt == 0 {
		return "", encodedLength, nil
	}

	payloadOffset := offset + StringASCIIHeaderLength
	if err := BoundsWrapError(buf, payloadOffset, strLenInt, ErrStringASCIIOutOfBounds); err != nil {
		return "", 0, err
	}
	return string(buf.Bytes()[payloadOffset : payloadOffset+strLenInt]), encodedLength, nil
}

func BoundsWrapError(buf AtomicBuffer, offset, length int, wrapped error) error {
	capacity := buf.Capacity()
	if !sliceRangeInBounds(capacity, offset, length) {
		return fmt.Errorf(
			"%w: offset=%d capacity=%d",
			wrapped,
			offset,
			capacity,
		)
	}
	return nil
}

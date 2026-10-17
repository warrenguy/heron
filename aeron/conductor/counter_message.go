package conductor

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
)

var (
	ErrCounterMessageOutOfBounds    = errors.New("conductor: counter message out of bounds")
	ErrCounterMessageLengthNegative = errors.New("conductor: counter message length negative")
	ErrCounterKeyTooLong            = fmt.Errorf(
		"conductor: counter key exceeds %d bytes",
		cnc.CounterMetadataMaxKeyLength,
	)
	ErrCounterLabelTooLong = fmt.Errorf(
		"conductor: counter label exceeds %d bytes",
		cnc.CounterMetadataMaxLabelLength,
	)
)

// CounterMessage is the to-driver body for ADD_COUNTER.
type CounterMessage struct {
	ClientID      int64
	CorrelationID int64
	TypeID        int32
	Key           []byte
	Label         string
}

// CounterMessageLabelLengthOffset returns the body offset of the label length
// for a key of keyLength bytes.
func CounterMessageLabelLengthOffset(keyLength int) int {
	return CounterMessageKeyOffset + atomicbuffer.Align(keyLength, sizeOfInt32)
}

// CounterMessageBodyLength returns the encoded ADD_COUNTER body length. The
// length ends at the last label byte. Java computeLength adds 4 more.
func CounterMessageBodyLength(keyLength, labelLength int) int {
	return CounterMessageLabelLengthOffset(keyLength) +
		atomicbuffer.StringASCIIEncodedLength(labelLength)
}

// EncodeCounterMessage writes an ADD_COUNTER body at offset.
//
// Returns an error when:
//   - key exceeds cnc.CounterMetadataMaxKeyLength: ErrCounterKeyTooLong
//   - label exceeds cnc.CounterMetadataMaxLabelLength: ErrCounterLabelTooLong
//   - label holds a non-ASCII byte: atomicbuffer.ErrStringASCIINonASCII
//   - the body does not fit buf: ErrCounterMessageOutOfBounds
//
// On error buf is untouched.
func EncodeCounterMessage(
	buf atomicbuffer.AtomicBuffer,
	offset int,
	clientID, correlationID int64,
	typeID int32,
	key []byte,
	label string,
) error {
	if err := validateCounterInput(key, label); err != nil {
		return err
	}
	bodyLength := CounterMessageBodyLength(len(key), len(label))
	if err := bodyBoundsError(
		buf,
		offset,
		bodyLength,
		ErrCounterMessageOutOfBounds,
	); err != nil {
		return err
	}

	putCorrelatedFields(buf, offset, clientID, correlationID)
	buf.PutInt32(offset+CounterMessageTypeIDOffset, typeID)
	//nolint:gosec // G115: CounterMetadataMaxKeyLength bounds the key length
	buf.PutInt32(offset+CounterMessageKeyLengthOffset, int32(len(key)))
	buf.PutBytes(offset+CounterMessageKeyOffset, key)
	labelLengthOffset := CounterMessageLabelLengthOffset(len(key))
	paddingOffset := CounterMessageKeyOffset + len(key)
	buf.SetBytes(offset+paddingOffset, labelLengthOffset-paddingOffset, 0)
	_, err := atomicbuffer.PutStringASCII(buf, offset+labelLengthOffset, label)
	return err
}

func validateCounterInput(key []byte, label string) error {
	if len(key) > cnc.CounterMetadataMaxKeyLength {
		return ErrCounterKeyTooLong
	}
	if len(label) > cnc.CounterMetadataMaxLabelLength {
		return ErrCounterLabelTooLong
	}
	return atomicbuffer.ValidateStringASCII(label)
}

// DecodeCounterMessage reads an ADD_COUNTER body of bodyLength bytes.
//
// Behaviour: accepts a body longer than the encoded length, as the Java driver
// does. Copies the key out of buf.
//
// Returns an error when:
//   - the body or a field runs past bodyLength: ErrCounterMessageOutOfBounds
//   - the key or label length is negative: ErrCounterMessageLengthNegative
//   - the key length exceeds cnc.CounterMetadataMaxKeyLength: ErrCounterKeyTooLong
//   - the label length exceeds cnc.CounterMetadataMaxLabelLength: ErrCounterLabelTooLong
func DecodeCounterMessage(
	buf atomicbuffer.AtomicBuffer,
	offset, bodyLength int,
) (CounterMessage, error) {
	if err := decodeMinimumBodyLengthError(
		bodyLength,
		CounterMessageMinimumBodyLength,
		ErrCounterMessageOutOfBounds,
	); err != nil {
		return CounterMessage{}, err
	}
	if err := bodyBoundsError(
		buf,
		offset,
		bodyLength,
		ErrCounterMessageOutOfBounds,
	); err != nil {
		return CounterMessage{}, err
	}

	keyLength, err := decodeCounterFieldLength(
		"keyLength",
		buf.GetInt32(offset+CounterMessageKeyLengthOffset),
		cnc.CounterMetadataMaxKeyLength,
		ErrCounterKeyTooLong,
	)
	if err != nil {
		return CounterMessage{}, err
	}

	labelLengthOffset := CounterMessageLabelLengthOffset(keyLength)
	if err := decodeMinimumBodyLengthError(
		bodyLength,
		labelLengthOffset+sizeOfInt32,
		ErrCounterMessageOutOfBounds,
	); err != nil {
		return CounterMessage{}, err
	}

	labelLength, err := decodeCounterFieldLength(
		"labelLength",
		buf.GetInt32(offset+labelLengthOffset),
		cnc.CounterMetadataMaxLabelLength,
		ErrCounterLabelTooLong,
	)
	if err != nil {
		return CounterMessage{}, err
	}

	if err := decodeMinimumBodyLengthError(
		bodyLength,
		CounterMessageBodyLength(keyLength, labelLength),
		ErrCounterMessageOutOfBounds,
	); err != nil {
		return CounterMessage{}, err
	}

	keyOffset := offset + CounterMessageKeyOffset
	key := bytes.Clone(buf.Bytes()[keyOffset : keyOffset+keyLength])
	label, _, err := atomicbuffer.GetStringASCII(buf, offset+labelLengthOffset)
	if err != nil {
		return CounterMessage{}, err
	}

	return CounterMessage{
		ClientID:      buf.GetInt64(offset + CorrelatedMessageClientIDOffset),
		CorrelationID: buf.GetInt64(offset + CorrelatedMessageCorrelationIDOffset),
		TypeID:        buf.GetInt32(offset + CounterMessageTypeIDOffset),
		Key:           key,
		Label:         label,
	}, nil
}

func decodeCounterFieldLength(
	field string,
	length int32,
	maximum int,
	tooLong error,
) (int, error) {
	if length < 0 {
		return 0, fmt.Errorf("%w: %s=%d", ErrCounterMessageLengthNegative, field, length)
	}
	if int(length) > maximum {
		return 0, fmt.Errorf("%w: length=%d", tooLong, length)
	}
	return int(length), nil
}

// CounterMessageBytes returns an encoded ADD_COUNTER body. Panics on input
// that EncodeCounterMessage rejects.
func CounterMessageBytes(
	clientID, correlationID int64,
	typeID int32,
	key []byte,
	label string,
) []byte {
	return encodeToBytes(
		CounterMessageBodyLength(len(key), len(label)),
		func(buf atomicbuffer.AtomicBuffer, offset int) error {
			return EncodeCounterMessage(buf, offset, clientID, correlationID, typeID, key, label)
		},
	)
}

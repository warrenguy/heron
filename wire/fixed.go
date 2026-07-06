package wire

import "github.com/warrenguy/heron/agrona/atomic"

// CheckLength reports whether length is at least minimum.
//
// length is the logical length supplied to a decode path (for example the Aeron
// payload length), not len(backing slice). A true result does not prove the
// backing slice is large enough for subsequent field reads at offset.
func CheckLength(length, minimum int) bool {
	return length >= minimum
}

// GetInt64 reads little-endian int64 from payload at offset.
func GetInt64(payload []byte, offset int) int64 {
	return atomic.Wrap(payload).GetInt64(offset)
}

// PutInt64 writes little-endian int64 to payload at offset.
func PutInt64(payload []byte, offset int, value int64) {
	atomic.Wrap(payload).PutInt64(offset, value)
}

func wireBoolFromByte(b byte) bool {
	return b != 0
}

// GetBoolByte reads one-byte wire bool: 0 is false, any non-zero is true.
func GetBoolByte(buf atomic.AtomicBuffer, offset int) bool {
	return wireBoolFromByte(buf.GetByte(offset))
}

// PutBoolByte writes 0 or 1 for false or true.
func PutBoolByte(buf atomic.AtomicBuffer, offset int, value bool) {
	buf.PutByte(offset, encodeWireBoolByte(value))
}

// GetBoolByteBytes reads one-byte wire bool from payload at offset.
func GetBoolByteBytes(payload []byte, offset int) bool {
	return GetBoolByte(atomic.Wrap(payload), offset)
}

// PutBoolByteBytes writes 0 or 1 for false or true to payload at offset.
func PutBoolByteBytes(payload []byte, offset int, value bool) {
	PutBoolByte(atomic.Wrap(payload), offset, value)
}

func encodeWireBoolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}

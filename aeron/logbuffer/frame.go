package logbuffer

import (
	"encoding/binary"
	"errors"

	"github.com/warrenguy/heron/aeron/atomic"
)

const (
	DataHeaderLength     = 32
	FrameHeaderVersion   = 0
	HdrTypePad           = 0
	HdrTypeData          = 1
	BeginFlag            = 0x80
	EndFlag              = 0x40
	EOSFlag              = 0x20
	RevokedFlag          = 0x10
	Unfragmented         = BeginFlag | EndFlag
	DefaultReservedValue = 0
	MaxMessageLength     = 16 * 1024 * 1024

	OffsetFrameLength   = 0
	OffsetVersion       = 4
	OffsetFlags         = 5
	OffsetFrameType     = 6
	OffsetTermOffset    = 8
	OffsetSessionID     = 12
	OffsetStreamID      = 16
	OffsetTermID        = 20
	OffsetReservedValue = 24
	OffsetFramePayload  = DataHeaderLength
)

func frameHeaderInBounds(buffer atomic.AtomicBuffer, frameOffset int) bool {
	return frameOffset >= 0 && frameOffset+DataHeaderLength <= buffer.Capacity()
}

// Frame header accessors use three OOB contracts:
//
// Read helpers return zero when the 32-byte header would extend past the buffer.
// IsPaddingFrame returns false on OOB (zero type is indistinguishable from PAD).
// Zero is indistinguishable from an uncommitted field; callers must ensure
// frameOffset lies within the term before polling.
//
// Put* mutators silently skip when the header would extend past the buffer.
// Void return mirrors Aeron Java (Agrona throws IndexOutOfBounds); hot-path
// layout code owns bounds, not these field stores.
//
// Write* helpers return an error on OOB. They compose multi-field header setup
// for publishers; setup mistakes must surface at initialization time.

// FrameLength returns frame length with a plain load. For tests or post-commit
// reads in the same goroutine only. Subscription poll on shared mmap must use
// FrameLengthVolatile with PutFrameLengthRelease/Ordered.
func FrameLength(buffer atomic.AtomicBuffer, frameOffset int) int32 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return buffer.GetInt32(frameOffset + OffsetFrameLength)
}

// FrameLengthVolatile acquire-loads frame length. Image/subscription poll loops
// on shared term buffers must use this accessor.
func FrameLengthVolatile(buffer atomic.AtomicBuffer, frameOffset int) int32 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return buffer.GetInt32Volatile(frameOffset + OffsetFrameLength)
}

func FrameVersion(buffer atomic.AtomicBuffer, frameOffset int) int8 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return int8(buffer.Bytes()[frameOffset+OffsetVersion])
}

func FrameFlags(buffer atomic.AtomicBuffer, frameOffset int) uint8 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return buffer.Bytes()[frameOffset+OffsetFlags]
}

func FrameType(buffer atomic.AtomicBuffer, frameOffset int) int16 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return int16(binary.LittleEndian.Uint16(buffer.Bytes()[frameOffset+OffsetFrameType:]))
}

func FrameTermOffset(buffer atomic.AtomicBuffer, frameOffset int) int32 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return buffer.GetInt32(frameOffset + OffsetTermOffset)
}

func FrameSessionID(buffer atomic.AtomicBuffer, frameOffset int) int32 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return buffer.GetInt32(frameOffset + OffsetSessionID)
}

func FrameStreamID(buffer atomic.AtomicBuffer, frameOffset int) int32 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return buffer.GetInt32(frameOffset + OffsetStreamID)
}

func FrameTermID(buffer atomic.AtomicBuffer, frameOffset int) int32 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return buffer.GetInt32(frameOffset + OffsetTermID)
}

func FrameReservedValue(buffer atomic.AtomicBuffer, frameOffset int) int64 {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return 0
	}
	return buffer.GetInt64(frameOffset + OffsetReservedValue)
}

func IsPaddingFrame(buffer atomic.AtomicBuffer, frameOffset int) bool {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return false
	}
	return int16(binary.LittleEndian.Uint16(
		buffer.Bytes()[frameOffset+OffsetFrameType:],
	)) == HdrTypePad
}

func putFrameLengthRelease(buffer atomic.AtomicBuffer, frameOffset int, frameLength int32) {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return
	}
	buffer.PutInt32Release(frameOffset+OffsetFrameLength, frameLength)
}

// PutFrameLengthOrdered publishes committed positive frame length via release store.
// Aeron exports Ordered and Release as separate entry points; Go has no ordered int32
// store, so both names share one release implementation.
func PutFrameLengthOrdered(buffer atomic.AtomicBuffer, frameOffset int, frameLength int32) {
	putFrameLengthRelease(buffer, frameOffset, frameLength)
}

// PutFrameLengthRelease marks an in-flight frame (negative length) via release store.
func PutFrameLengthRelease(buffer atomic.AtomicBuffer, frameOffset int, frameLength int32) {
	putFrameLengthRelease(buffer, frameOffset, frameLength)
}

func PutFrameType(buffer atomic.AtomicBuffer, frameOffset int, typ int16) {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return
	}
	binary.LittleEndian.PutUint16(buffer.Bytes()[frameOffset+OffsetFrameType:], uint16(typ))
}

func PutFrameFlags(buffer atomic.AtomicBuffer, frameOffset int, flags uint8) {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return
	}
	buffer.Bytes()[frameOffset+OffsetFlags] = flags
}

func PutFrameTermOffset(buffer atomic.AtomicBuffer, frameOffset int, termOffset int32) {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return
	}
	buffer.PutInt32(frameOffset+OffsetTermOffset, termOffset)
}

func PutFrameSessionID(buffer atomic.AtomicBuffer, frameOffset int, sessionID int32) {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return
	}
	buffer.PutInt32(frameOffset+OffsetSessionID, sessionID)
}

func PutFrameStreamID(buffer atomic.AtomicBuffer, frameOffset int, streamID int32) {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return
	}
	buffer.PutInt32(frameOffset+OffsetStreamID, streamID)
}

func PutFrameTermID(buffer atomic.AtomicBuffer, frameOffset int, termID int32) {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return
	}
	buffer.PutInt32(frameOffset+OffsetTermID, termID)
}

func PutFrameReservedValue(buffer atomic.AtomicBuffer, frameOffset int, value int64) {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return
	}
	buffer.PutInt64(frameOffset+OffsetReservedValue, value)
}

func writeFrameHeader(
	buffer atomic.AtomicBuffer,
	frameOffset int,
	flags uint8,
	typ int16,
	termID int32,
	termOffset int32,
) error {
	if !frameHeaderInBounds(buffer, frameOffset) {
		return errors.New("logbuffer: frame header out of bounds")
	}
	header := buffer.Bytes()
	header[frameOffset+OffsetVersion] = byte(FrameHeaderVersion)
	PutFrameFlags(buffer, frameOffset, flags)
	PutFrameType(buffer, frameOffset, typ)
	buffer.PutInt32(frameOffset+OffsetTermID, termID)
	buffer.PutInt32(frameOffset+OffsetTermOffset, termOffset)
	buffer.PutInt64(frameOffset+OffsetReservedValue, DefaultReservedValue)
	return nil
}

func writeDataHeader(
	buffer atomic.AtomicBuffer,
	frameOffset int,
	flags uint8,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
) error {
	if err := writeFrameHeader(
		buffer,
		frameOffset,
		flags,
		HdrTypeData,
		termID,
		termOffset,
	); err != nil {
		return err
	}
	buffer.PutInt32(frameOffset+OffsetSessionID, sessionID)
	buffer.PutInt32(frameOffset+OffsetStreamID, streamID)
	return nil
}

func WriteDataHeaderUnfragmented(
	buffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
) error {
	return writeDataHeader(
		buffer,
		frameOffset,
		flags|Unfragmented,
		sessionID,
		streamID,
		termID,
		termOffset,
	)
}

// WriteDataHeaderFragmented sets DATA header fields with caller fragment flags
// (BeginFlag, EndFlag, or neither). Never passes Unfragmented (0xC0). Caller
// commits payload visibility via PutFrameLengthOrdered after copying bytes.
func WriteDataHeaderFragmented(
	buffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	fragmentFlags uint8,
) error {
	return writeDataHeader(
		buffer,
		frameOffset,
		fragmentFlags,
		sessionID,
		streamID,
		termID,
		termOffset,
	)
}

func WritePaddingFrame(
	buffer atomic.AtomicBuffer,
	frameOffset int,
	termID int32,
	termOffset int32,
) error {
	return writeFrameHeader(buffer, frameOffset, 0, HdrTypePad, termID, termOffset)
}

// CommitMinimalTermEndPadding writes frame length and PAD type into whatever
// header fields fit when the tail gap is shorter than DataHeaderLength.
func CommitMinimalTermEndPadding(
	termBuf atomic.AtomicBuffer,
	termOffset int,
	paddingLength int32,
) {
	if paddingLength <= 0 {
		return
	}

	paddingEnd := termOffset + int(paddingLength)
	if paddingEnd > termBuf.Capacity() {
		return
	}

	if termOffset+OffsetFrameLength+4 <= paddingEnd {
		termBuf.PutInt32Release(termOffset+OffsetFrameLength, paddingLength)
	}
	if termOffset+OffsetFrameType+2 <= paddingEnd {
		binary.LittleEndian.PutUint16(
			termBuf.Bytes()[termOffset+OffsetFrameType:],
			uint16(HdrTypePad),
		)
	}
}

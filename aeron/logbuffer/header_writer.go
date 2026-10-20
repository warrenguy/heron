package logbuffer

import (
	"unsafe"

	"github.com/warrenguy/heron/atomicbuffer"
)

// HeaderWriter writes DATA frame headers as three 64-bit words, like Java
// HeaderWriter. The words hold the fields that stay fixed for one
// publication.
//
// HeaderWriter does not write the reserved value at OffsetReservedValue. The
// driver zeroes the term before reuse.
type HeaderWriter struct {
	// versionFlagsType holds the template version, flags, and type in the
	// high 32 bits of the word at OffsetFrameLength.
	versionFlagsType uint64

	// sessionID holds the session id in the high 32 bits of the word at
	// OffsetTermOffset.
	sessionID uint64

	// streamID holds the stream id in the low 32 bits of the word at
	// OffsetStreamID.
	streamID uint64
}

// headerFlagsMask selects the flags byte at OffsetFlags in the
// versionFlagsType word.
const headerFlagsMask = uint64(0xFF) << ((OffsetFlags - OffsetFrameLength) * 8)

// NewHeaderWriter reads version, flags, and type from template. sessionID
// and streamID replace the template values.
//
// A template shorter than DataHeaderLength reads as zero past its end. A
// template too short to hold the version byte gets FrameHeaderVersion.
func NewHeaderWriter(
	template atomicbuffer.AtomicBuffer,
	sessionID int32,
	streamID int32,
) HeaderWriter {
	var header [DataHeaderLength]byte

	templateCapacity := template.Capacity()
	copyLength := min(templateCapacity, DataHeaderLength)
	if copyLength < DataHeaderLength {
		header[OffsetVersion] = byte(FrameHeaderVersion)
	}
	if copyLength > 0 {
		copy(header[:], template.Bytes()[:copyLength])
	}

	versionFlagsType := atomicbuffer.Wrap(header[:]).GetInt32(OffsetVersion)
	return HeaderWriter{
		versionFlagsType: wordBits(versionFlagsType) << 32,
		sessionID:        wordBits(sessionID) << 32,
		streamID:         wordBits(streamID),
	}
}

// wordBits returns the two's complement bits of value in the low 32 bits of a
// header word.
func wordBits(value int32) uint64 {
	//nolint:gosec // G115: keeps the two's complement bit pattern
	return uint64(uint32(value))
}

// Write writes the header at termOffset with the template flags. See
// WriteHeader for the store order and the alignment rule.
//
// Panics when the header does not fit in termBuf.
func (writer *HeaderWriter) Write(
	termBuf atomicbuffer.AtomicBuffer,
	termOffset int,
	frameLength int32,
	termID int32,
) {
	frame := termBuf.Bytes()[termOffset : termOffset+DataHeaderLength]
	writer.WriteHeader(frame, termOffset, frameLength, termID)
}

// WriteWithFlags writes the header at termOffset with flags in place of the
// template flags. See WriteHeader for the store order and the alignment rule.
//
// Panics when the header does not fit in termBuf.
func (writer *HeaderWriter) WriteWithFlags(
	termBuf atomicbuffer.AtomicBuffer,
	termOffset int,
	frameLength int32,
	termID int32,
	flags uint8,
) {
	flagged := *writer
	flagged.versionFlagsType = writer.versionFlagsType&^headerFlagsMask |
		uint64(flags)<<((OffsetFlags-OffsetFrameLength)*8)
	frame := termBuf.Bytes()[termOffset : termOffset+DataHeaderLength]
	flagged.WriteHeader(frame, termOffset, frameLength, termID)
}

// WriteHeader writes the header with the template flags at the start of frame.
// frame starts at termOffset in the term buffer. The offer and claim paths
// call it on a frame they have already sliced, so the header does not need
// a second bounds check.
//
// Store order:
//  1. A release store of the word at OffsetFrameLength. It holds -frameLength
//     with version, flags, and type.
//  2. A store-store fence.
//  3. Plain stores of the words at OffsetTermOffset and OffsetStreamID.
//
// Version, flags, and type become visible with the negative frame length. The
// other fields become visible after it.
//
// frame must start at a FrameAlignment-aligned offset in a term buffer with an
// 8-byte aligned base. The first store is a 64-bit atomic store, and arm64
// faults on a misaligned one.
//
// Panics when frame is shorter than DataHeaderLength.
func (writer *HeaderWriter) WriteHeader(
	frame []byte,
	termOffset int,
	frameLength int32,
	termID int32,
) {
	_ = frame[DataHeaderLength-1]

	//nolint:gosec // G115: keeps the two's complement bit pattern
	atomicbuffer.Wrap(frame).PutInt64ReleaseStoreFenceAt(
		OffsetFrameLength,
		int64(writer.versionFlagsType|uint64(uint32(-frameLength))),
	)

	// The words go out in host order, which is little-endian on amd64 and arm64.
	//nolint:gosec // G103: the frame index above proves 32 bytes from frame start
	header := unsafe.Pointer(unsafe.SliceData(frame))

	//nolint:gosec // G115: the term offset is below the term length
	*(*uint64)(unsafe.Add(header, OffsetTermOffset)) = writer.sessionID | uint64(uint32(termOffset))

	//nolint:gosec // G115: keeps the two's complement bit pattern
	*(*uint64)(unsafe.Add(header, OffsetStreamID)) = uint64(uint32(termID))<<32 | writer.streamID
}

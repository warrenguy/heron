package logbuffer

import (
	"github.com/warrenguy/heron/aeron/atomic"
)

type HeaderWriter struct {
	header [DataHeaderLength]byte
}

func NewHeaderWriter(
	template atomic.AtomicBuffer,
	sessionID int32,
	streamID int32,
) HeaderWriter {
	var writer HeaderWriter

	templateCapacity := template.Capacity()
	copyLength := min(templateCapacity, DataHeaderLength)
	if copyLength < DataHeaderLength {
		writer.header[OffsetVersion] = byte(FrameHeaderVersion)
	}
	if copyLength > 0 {
		copy(writer.header[:], template.Bytes()[:copyLength])
	}

	atomicBuf := atomic.Wrap(writer.header[:])
	PutFrameSessionID(atomicBuf, 0, sessionID)
	PutFrameStreamID(atomicBuf, 0, streamID)

	return writer
}

func (writer *HeaderWriter) Write(
	termBuf atomic.AtomicBuffer,
	termOffset int,
	frameLength int32,
	termID int32,
) {
	writer.writeHeader(termBuf, termOffset, frameLength, termID, writer.header[OffsetFlags])
}

func (writer *HeaderWriter) WriteWithFlags(
	termBuf atomic.AtomicBuffer,
	termOffset int,
	frameLength int32,
	termID int32,
	flags uint8,
) {
	writer.writeHeader(termBuf, termOffset, frameLength, termID, flags)
}

func (writer *HeaderWriter) writeHeader(
	termBuf atomic.AtomicBuffer,
	termOffset int,
	frameLength int32,
	termID int32,
	flags uint8,
) {
	PutFrameLengthRelease(termBuf, termOffset, -frameLength)
	atomic.ReleaseFence()
	termBuf.PutBytes(termOffset+OffsetVersion, writer.header[OffsetVersion:OffsetFlags])
	PutFrameFlags(termBuf, termOffset, flags)
	termBuf.PutBytes(termOffset+OffsetFrameType, writer.header[OffsetFrameType:OffsetTermOffset])
	termBuf.PutBytes(termOffset+OffsetSessionID, writer.header[OffsetSessionID:OffsetTermID])
	termBuf.PutBytes(termOffset+OffsetReservedValue, writer.header[OffsetReservedValue:])
	PutFrameTermID(termBuf, termOffset, termID)
	PutFrameTermOffset(termBuf, termOffset, int32(termOffset))
}

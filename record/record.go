package record

import (
	"errors"

	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	TypeFieldOffset  = atomicbuffer.SizeInt32
	HeaderLength     = atomicbuffer.SizeInt32 + atomicbuffer.SizeInt32
	Alignment        = HeaderLength
	PaddingMsgTypeID = -1

	// InsufficientCapacity is the Agrona ManyToOneRingBuffer claim index when the
	// ring has no space for the requested record. Buffer layout or mmap size errors
	// at construction use package-specific errors (e.g. ringbuffer CheckCapacity),
	// not this sentinel.
	InsufficientCapacity = -2
)

var ErrInvalidMessageTypeID = errors.New("record: message type id must be greater than zero")

func LengthOffset(recordOffset int) int {
	return recordOffset
}

func TypeOffset(recordOffset int) int {
	return recordOffset + TypeFieldOffset
}

func EncodedMsgOffset(recordOffset int) int {
	return recordOffset + HeaderLength
}

func AlignRecordLength(recordLength int) int {
	return (recordLength + Alignment - 1) & -Alignment
}

func CheckTypeID(msgTypeID int) error {
	if msgTypeID < 1 {
		return ErrInvalidMessageTypeID
	}
	return nil
}

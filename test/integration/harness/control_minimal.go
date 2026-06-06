package harness

import (
	"errors"
	"fmt"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	clientKeepaliveTypeID               = 0x06
	correlatedMessageLength             = 16
	getNextAvailableSessionIDTypeID     = 0x12
	onNextAvailableSessionIDTypeID      = 0x0F0D
	getNextAvailableSessionIDBodyLength = 20
	onNextAvailableSessionIDBodyLength  = 12
)

var errRingInsufficientCapacity = errors.New("harness: ring insufficient capacity")

func encodeCorrelated(buffer aeronatomic.AtomicBuffer, offset int, clientID, correlationID int64) {
	buffer.PutInt64(offset, clientID)
	buffer.PutInt64(offset+8, correlationID)
}

func claimRingMessage(
	ring *ringbuffer.ManyToOneRingBuffer,
	msgTypeID, length int,
) (int, error) {
	encodedOffset, err := ring.TryClaim(msgTypeID, length)
	if err != nil {
		return 0, err
	}
	if encodedOffset == ringbuffer.InsufficientCapacity {
		return 0, errRingInsufficientCapacity
	}
	return encodedOffset, nil
}

func offerClientKeepalive(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, correlationID int64,
) error {
	encodedOffset, err := claimRingMessage(ring, clientKeepaliveTypeID, correlatedMessageLength)
	if err != nil {
		return err
	}

	encodeCorrelated(ring.Buffer(), encodedOffset, clientID, correlationID)
	return ring.Commit(encodedOffset)
}

func offerGetNextSessionID(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, correlationID int64,
	streamID int32,
) error {
	encodedOffset, err := claimRingMessage(
		ring,
		getNextAvailableSessionIDTypeID,
		getNextAvailableSessionIDBodyLength,
	)
	if err != nil {
		return err
	}

	buffer := ring.Buffer()
	encodeCorrelated(buffer, encodedOffset, clientID, correlationID)
	buffer.PutInt32(encodedOffset+16, streamID)
	return ring.Commit(encodedOffset)
}

type NextSessionIDEvent struct {
	CorrelationID int64
	NextSessionID int32
}

func decodeNextSessionIDEvent(
	buffer aeronatomic.AtomicBuffer,
	offset int,
) (NextSessionIDEvent, error) {
	if buffer.Capacity() < offset+onNextAvailableSessionIDBodyLength {
		return NextSessionIDEvent{}, fmt.Errorf(
			"harness: next session id payload out of bounds: offset=%d capacity=%d",
			offset,
			buffer.Capacity(),
		)
	}
	return NextSessionIDEvent{
		CorrelationID: buffer.GetInt64(offset),
		NextSessionID: buffer.GetInt32(offset + 8),
	}, nil
}

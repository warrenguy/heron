package harness

import (
	"errors"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

var (
	errRingInsufficientCapacity = errors.New("harness: ring insufficient capacity")
	errCorrelatedBodyTooShort   = errors.New(
		"harness: correlated body length shorter than correlated header",
	)
)

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

func offerCorrelatedCommand(
	ring *ringbuffer.ManyToOneRingBuffer,
	msgTypeID, bodyLength int,
	clientID, correlationID int64,
	writeTail func(buffer aeronatomic.AtomicBuffer, offset int),
) error {
	if bodyLength < conductor.CorrelatedMessageLength {
		return errCorrelatedBodyTooShort
	}

	encodedOffset, err := claimRingMessage(ring, msgTypeID, bodyLength)
	if err != nil {
		return err
	}

	buffer := ring.Buffer()
	if err := conductor.EncodeCorrelated(buffer, encodedOffset, clientID, correlationID); err != nil {
		_ = ring.Abort(encodedOffset)
		return err
	}
	if writeTail != nil {
		writeTail(buffer, encodedOffset)
	}
	return ring.Commit(encodedOffset)
}

func offerClientKeepalive(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, correlationID int64,
) error {
	return offerCorrelatedCommand(
		ring,
		conductor.ClientKeepalive,
		conductor.CorrelatedMessageLength,
		clientID,
		correlationID,
		nil,
	)
}

func offerGetNextSessionID(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, correlationID int64,
	streamID int32,
) error {
	return offerCorrelatedCommand(
		ring,
		conductor.GetNextAvailableSessionID,
		conductor.GetNextAvailableSessionIDBodyLength,
		clientID,
		correlationID,
		func(buffer aeronatomic.AtomicBuffer, offset int) {
			buffer.PutInt32(offset+conductor.CorrelatedMessageLength, streamID)
		},
	)
}

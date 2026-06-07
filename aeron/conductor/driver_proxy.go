package conductor

import (
	"errors"

	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	NullCorrelationID       = int64(-1)
	DriverShutdownHeartbeat = NullCorrelationID
)

var ErrRingInsufficientCapacity = errors.New("conductor: ring insufficient capacity")

type DriverProxy struct {
	ring     *ringbuffer.ManyToOneRingBuffer
	clientID int64
}

func NewDriverProxy(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
) *DriverProxy {
	return &DriverProxy{
		ring:     ring,
		clientID: clientID,
	}
}

func (proxy *DriverProxy) TimeOfLastDriverKeepaliveMs() int64 {
	return proxy.ring.ConsumerHeartbeatTime()
}

func (proxy *DriverProxy) ClientKeepalive() error {
	return proxy.offerCorrelated(ClientKeepalive, false)
}

func (proxy *DriverProxy) ClientClose() {
	_ = proxy.offerCorrelated(ClientClose, true)
}

func (proxy *DriverProxy) GetNextAvailableSessionID(
	correlationID int64,
	streamID int32,
) error {
	return proxy.offerCorrelatedBody(
		GetNextAvailableSessionID,
		GetNextAvailableSessionIDBodyLength,
		correlationID,
		false,
		func(encodedOffset int) {
			proxy.ring.Buffer().PutInt32(encodedOffset+CorrelatedMessageLength, streamID)
		},
	)
}

func (proxy *DriverProxy) offerCorrelated(msgTypeID int, silent bool) error {
	return proxy.offerCorrelatedBody(
		msgTypeID,
		CorrelatedMessageLength,
		NullCorrelationID,
		silent,
		nil,
	)
}

func (proxy *DriverProxy) offerCorrelatedBody(
	msgTypeID int,
	bodyLength int,
	correlationID int64,
	silent bool,
	afterEncode func(encodedOffset int),
) error {
	if bodyLength < CorrelatedMessageLength {
		return ErrCorrelatedBodyTooShort
	}

	encodedOffset, err := proxy.ring.TryClaim(msgTypeID, bodyLength)
	if err != nil {
		return err
	}
	if encodedOffset == ringbuffer.InsufficientCapacity {
		if silent {
			return nil
		}
		return ErrRingInsufficientCapacity
	}

	return proxy.commitCorrelatedAt(
		encodedOffset,
		encodedOffset,
		correlationID,
		afterEncode,
	)
}

func (proxy *DriverProxy) encodeCommitCorrelatedAt(
	claimOffset int,
	encodeOffset int,
) error {
	return proxy.commitCorrelatedAt(claimOffset, encodeOffset, NullCorrelationID, nil)
}

func (proxy *DriverProxy) commitCorrelatedAt(
	claimOffset int,
	encodeOffset int,
	correlationID int64,
	afterEncode func(encodedOffset int),
) error {
	buffer := proxy.ring.Buffer()
	if err := EncodeCorrelated(
		buffer,
		encodeOffset,
		proxy.clientID,
		correlationID,
	); err != nil {
		_ = proxy.ring.Abort(claimOffset)
		return err
	}
	if afterEncode != nil {
		afterEncode(encodeOffset)
	}
	return proxy.ring.Commit(claimOffset)
}

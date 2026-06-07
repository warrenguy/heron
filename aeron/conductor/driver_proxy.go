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

func (proxy *DriverProxy) offerCorrelated(msgTypeID int, silent bool) error {
	encodedOffset, err := proxy.ring.TryClaim(msgTypeID, CorrelatedMessageLength)
	if err != nil {
		return err
	}
	if encodedOffset == ringbuffer.InsufficientCapacity {
		if silent {
			return nil
		}
		return ErrRingInsufficientCapacity
	}
	return proxy.encodeCommitCorrelatedAt(encodedOffset, encodedOffset)
}

func (proxy *DriverProxy) encodeCommitCorrelatedAt(
	claimOffset int,
	encodeOffset int,
) error {
	buffer := proxy.ring.Buffer()
	if err := EncodeCorrelated(
		buffer,
		encodeOffset,
		proxy.clientID,
		NullCorrelationID,
	); err != nil {
		_ = proxy.ring.Abort(claimOffset)
		return err
	}
	return proxy.ring.Commit(claimOffset)
}

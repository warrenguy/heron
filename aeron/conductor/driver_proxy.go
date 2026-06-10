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

func (proxy *DriverProxy) NextCorrelationID() int64 {
	return proxy.ring.NextCorrelationID()
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
		func(encodedOffset int) error {
			buf := proxy.ring.Buffer()
			streamOffset := encodedOffset + CorrelatedMessageLength
			if err := bodyBoundsError(
				buf,
				streamOffset,
				sizeOfInt32,
				ErrCorrelationAndInt32OutOfBounds,
			); err != nil {
				return err
			}
			buf.PutInt32(streamOffset, streamID)
			return nil
		},
	)
}

// AddPublication offers ADD_PUBLICATION on the to-driver ring.
// Channel must be ASCII; other bytes return atomic.ErrStringASCIINonASCII.
// Java Agrona replaces high bytes with '?'.
func (proxy *DriverProxy) AddPublication(
	correlationID int64,
	streamID int32,
	channel string,
) error {
	return proxy.offerBody(
		AddPublication,
		PublicationMessageBodyLength(channel),
		false,
		func(encodedOffset int) error {
			return EncodePublicationMessage(
				proxy.ring.Buffer(),
				encodedOffset,
				proxy.clientID,
				correlationID,
				streamID,
				channel,
			)
		},
	)
}

// AddSubscription offers ADD_SUBSCRIPTION on the to-driver ring.
// Channel must be ASCII; other bytes return atomic.ErrStringASCIINonASCII.
// Java Agrona replaces high bytes with '?'.
func (proxy *DriverProxy) AddSubscription(
	correlationID int64,
	streamID int32,
	channel string,
) error {
	return proxy.offerBody(
		AddSubscription,
		SubscriptionMessageBodyLength(channel),
		false,
		func(encodedOffset int) error {
			return EncodeSubscriptionMessage(
				proxy.ring.Buffer(),
				encodedOffset,
				proxy.clientID,
				correlationID,
				NullRegistrationCorrelationID,
				streamID,
				channel,
			)
		},
	)
}

func (proxy *DriverProxy) RemovePublication(
	correlationID, registrationID int64,
	revoke bool,
) error {
	return proxy.offerBody(
		RemovePublication,
		RemovePublicationBodyLength,
		false,
		func(encodedOffset int) error {
			return EncodeRemovePublicationMessage(
				proxy.ring.Buffer(),
				encodedOffset,
				proxy.clientID,
				correlationID,
				registrationID,
				revoke,
			)
		},
	)
}

func (proxy *DriverProxy) RemoveSubscription(
	correlationID, registrationID int64,
) error {
	return proxy.offerBody(
		RemoveSubscription,
		RemoveMessageBodyLength,
		false,
		func(encodedOffset int) error {
			return EncodeRemoveMessage(
				proxy.ring.Buffer(),
				encodedOffset,
				proxy.clientID,
				correlationID,
				registrationID,
			)
		},
	)
}

func (proxy *DriverProxy) offerBody(
	msgTypeID int,
	bodyLength int,
	silent bool,
	encode func(encodedOffset int) error,
) error {
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
	if err := encode(encodedOffset); err != nil {
		// In-flight TryClaim slot; abort is expected to succeed before returning encode error.
		if abortErr := proxy.ring.Abort(encodedOffset); abortErr != nil {
			return errors.Join(err, abortErr)
		}
		return err
	}
	return proxy.ring.Commit(encodedOffset)
}

func (proxy *DriverProxy) offerCorrelatedBody(
	msgTypeID int,
	bodyLength int,
	correlationID int64,
	silent bool,
	afterEncode func(encodedOffset int) error,
) error {
	if bodyLength < CorrelatedMessageLength {
		return ErrCorrelatedBodyTooShort
	}

	return proxy.offerBody(msgTypeID, bodyLength, silent, func(encodedOffset int) error {
		if err := EncodeCorrelated(
			proxy.ring.Buffer(),
			encodedOffset,
			proxy.clientID,
			correlationID,
		); err != nil {
			return err
		}
		if afterEncode != nil {
			if err := afterEncode(encodedOffset); err != nil {
				return err
			}
		}
		return nil
	})
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

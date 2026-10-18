package conductor

import (
	"errors"

	"github.com/warrenguy/heron/ringbuffer"
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

func (proxy *DriverProxy) LastDriverKeepaliveMs() int64 {
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

func (proxy *DriverProxy) NextAvailableSessionID(
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
//
// Channel must be ASCII; other bytes return atomicbuffer.ErrStringASCIINonASCII.
//
// Agrona putStringAscii replaces high bytes with '?'; encode rejects non-ASCII.
func (proxy *DriverProxy) AddPublication(
	correlationID int64,
	streamID int32,
	channel string,
) error {
	return proxy.offerPublicationMessage(AddPublication, correlationID, streamID, channel)
}

// AddExclusivePublication offers ADD_EXCLUSIVE_PUBLICATION on the to-driver ring.
//
// See AddPublication for channel encoding constraints.
func (proxy *DriverProxy) AddExclusivePublication(
	correlationID int64,
	streamID int32,
	channel string,
) error {
	return proxy.offerPublicationMessage(AddExclusivePublication, correlationID, streamID, channel)
}

// AddSubscription offers ADD_SUBSCRIPTION on the to-driver ring.
//
// Channel must be ASCII; other bytes return atomicbuffer.ErrStringASCIINonASCII.
//
// Agrona putStringAscii replaces high bytes with '?'; encode rejects non-ASCII.
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
	return proxy.offerRemoveMessage(RemoveSubscription, correlationID, registrationID)
}

// AddCounter offers ADD_COUNTER on the to-driver ring.
//
// Behaviour: validates the key and label before claiming a ring slot.
//
// Returns an error when:
//   - key exceeds cnc.CounterMetadataMaxKeyLength: ErrCounterKeyTooLong
//   - label exceeds cnc.CounterMetadataMaxLabelLength: ErrCounterLabelTooLong
//   - label holds a non-ASCII byte: atomicbuffer.ErrStringASCIINonASCII
//   - the ring is full: ErrRingInsufficientCapacity
func (proxy *DriverProxy) AddCounter(
	correlationID int64,
	typeID int32,
	key []byte,
	label string,
) error {
	if err := ValidateCounterInput(key, label); err != nil {
		return err
	}
	return proxy.offerBody(
		AddCounter,
		CounterMessageBodyLength(len(key), len(label)),
		false,
		func(encodedOffset int) error {
			return EncodeCounterMessage(
				proxy.ring.Buffer(),
				encodedOffset,
				proxy.clientID,
				correlationID,
				typeID,
				key,
				label,
			)
		},
	)
}

// RemoveCounter offers REMOVE_COUNTER on the to-driver ring. registrationID
// is the ADD_COUNTER correlation id. A full ring returns
// ErrRingInsufficientCapacity.
func (proxy *DriverProxy) RemoveCounter(
	correlationID, registrationID int64,
) error {
	return proxy.offerRemoveMessage(RemoveCounter, correlationID, registrationID)
}

func (proxy *DriverProxy) offerRemoveMessage(
	msgTypeID int,
	correlationID, registrationID int64,
) error {
	return proxy.offerBody(
		msgTypeID,
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

// AddReceiveDestination offers ADD_RCV_DESTINATION on the to-driver ring.
//
// registrationID is the subscription registration id. Channel must be ASCII.
// Other bytes return atomicbuffer.ErrStringASCIINonASCII.
func (proxy *DriverProxy) AddReceiveDestination(
	correlationID, registrationID int64,
	channel string,
) error {
	return proxy.offerDestinationMessage(
		AddReceiveDestination,
		correlationID,
		registrationID,
		channel,
	)
}

// RemoveReceiveDestination offers REMOVE_RCV_DESTINATION on the to-driver ring.
//
// See AddReceiveDestination for channel encoding constraints.
func (proxy *DriverProxy) RemoveReceiveDestination(
	correlationID, registrationID int64,
	channel string,
) error {
	return proxy.offerDestinationMessage(
		RemoveReceiveDestination,
		correlationID,
		registrationID,
		channel,
	)
}

// AddDestination offers ADD_DESTINATION on the to-driver ring.
//
// registrationID is the publication's original registration id. Channel must
// be ASCII. Other bytes return atomicbuffer.ErrStringASCIINonASCII.
func (proxy *DriverProxy) AddDestination(
	correlationID, registrationID int64,
	channel string,
) error {
	return proxy.offerDestinationMessage(
		AddDestination,
		correlationID,
		registrationID,
		channel,
	)
}

// RemoveDestination offers REMOVE_DESTINATION on the to-driver ring.
//
// See AddDestination for the registration id and channel encoding.
func (proxy *DriverProxy) RemoveDestination(
	correlationID, registrationID int64,
	channel string,
) error {
	return proxy.offerDestinationMessage(
		RemoveDestination,
		correlationID,
		registrationID,
		channel,
	)
}

// RemoveDestinationByID offers REMOVE_DESTINATION_BY_ID on the to-driver ring.
//
// resourceRegistrationID is the publication's original registration id.
// destinationRegistrationID is the correlation id of the ADD_DESTINATION
// command. A full ring returns ErrRingInsufficientCapacity.
func (proxy *DriverProxy) RemoveDestinationByID(
	correlationID, resourceRegistrationID, destinationRegistrationID int64,
) error {
	return proxy.offerBody(
		RemoveDestinationByID,
		DestinationByIDMessageBodyLength,
		false,
		func(encodedOffset int) error {
			return EncodeDestinationByIDMessage(
				proxy.ring.Buffer(),
				encodedOffset,
				proxy.clientID,
				correlationID,
				resourceRegistrationID,
				destinationRegistrationID,
			)
		},
	)
}

// offerDestinationMessage offers msgTypeID with the DestinationMessage wire
// layout. Send and receive destination commands share that layout.
func (proxy *DriverProxy) offerDestinationMessage(
	msgTypeID int,
	correlationID, registrationID int64,
	channel string,
) error {
	return proxy.offerBody(
		msgTypeID,
		DestinationMessageBodyLength(channel),
		false,
		func(encodedOffset int) error {
			return EncodeDestinationMessage(
				proxy.ring.Buffer(),
				encodedOffset,
				proxy.clientID,
				correlationID,
				registrationID,
				channel,
			)
		},
	)
}

// offerPublicationMessage offers a PublicationMessage-bodied command (ADD_PUBLICATION
// or ADD_EXCLUSIVE_PUBLICATION, which share wire layout) under the given message type.
func (proxy *DriverProxy) offerPublicationMessage(
	msgTypeID int,
	correlationID int64,
	streamID int32,
	channel string,
) error {
	return proxy.offerBody(
		msgTypeID,
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

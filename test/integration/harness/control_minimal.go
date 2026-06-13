package harness

import (
	"fmt"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

func decodeMatchingClientClose(
	buffer aeronatomic.AtomicBuffer,
	payloadOffset int,
	bodyLength int,
	clientID int64,
) (conductor.CorrelatedMessage, bool, error) {
	if bodyLength < conductor.CorrelatedMessageLength {
		return conductor.CorrelatedMessage{}, false, fmt.Errorf(
			"harness: client close body length %d shorter than correlated header",
			bodyLength,
		)
	}

	decoded, err := conductor.DecodeCorrelated(buffer, payloadOffset)
	if err != nil {
		return conductor.CorrelatedMessage{}, false, err
	}
	if decoded.ClientID != clientID ||
		decoded.CorrelationID != conductor.NullCorrelationID {
		return conductor.CorrelatedMessage{}, false, nil
	}

	return decoded, true, nil
}

func syntheticClientClose(clientID int64) conductor.CorrelatedMessage {
	return conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}
}

func clientCloseObserveSpec(clientID int64) toDriverObserveSpec[conductor.CorrelatedMessage] {
	return toDriverObserveSpec[conductor.CorrelatedMessage]{
		messageTypeID: conductor.ClientClose,
		recordLength:  conductor.CorrelatedRingRecordLength(),
		decodeMatch: func(
			buffer aeronatomic.AtomicBuffer,
			payloadOffset, bodyLength int,
		) (conductor.CorrelatedMessage, bool, error) {
			return decodeMatchingClientClose(buffer, payloadOffset, bodyLength, clientID)
		},
		clearedValue: func() conductor.CorrelatedMessage {
			return syntheticClientClose(clientID)
		},
	}
}

func findPendingClientClose(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
) (conductor.CorrelatedMessage, bool, error) {
	return findPendingOnRing(ring, clientCloseObserveSpec(clientID))
}

func findOfferedClientCloseAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
	producerBefore int64,
) (conductor.CorrelatedMessage, bool, error) {
	return findOfferedAt(ring, producerBefore, clientCloseObserveSpec(clientID), nil)
}

func observeClientClose(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
	consumerBefore, producerBefore int64,
	provenClientClosePosition *int64,
) (conductor.CorrelatedMessage, bool, error) {
	return observeOnRing(
		ring,
		consumerBefore,
		producerBefore,
		provenClientClosePosition,
		clientCloseObserveSpec(clientID),
	)
}

func findConsumedClientCloseAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
	consumerBefore, consumerAfter int64,
	producerBefore, producerAfter int64,
	provenClientClosePosition int64,
	onlyClientCloseOfferedSinceSnapshot bool,
) (conductor.CorrelatedMessage, bool, error) {
	return findConsumedAt(
		ring,
		consumerBefore,
		consumerAfter,
		producerBefore,
		producerAfter,
		provenClientClosePosition,
		onlyClientCloseOfferedSinceSnapshot,
		clientCloseObserveSpec(clientID),
	)
}

func decodeMatchingAddPublication(
	buffer aeronatomic.AtomicBuffer,
	payloadOffset int,
	bodyLength int,
	want conductor.PublicationMessage,
) (conductor.PublicationMessage, bool, error) {
	decoded, err := conductor.DecodePublicationMessage(buffer, payloadOffset, bodyLength)
	if err != nil {
		return conductor.PublicationMessage{}, false, err
	}
	if decoded.ClientID != want.ClientID ||
		decoded.CorrelationID != want.CorrelationID ||
		decoded.StreamID != want.StreamID ||
		decoded.Channel != want.Channel {
		return conductor.PublicationMessage{}, false, nil
	}

	return decoded, true, nil
}

func addPublicationObserveSpec(
	want conductor.PublicationMessage,
) toDriverObserveSpec[conductor.PublicationMessage] {
	return toDriverObserveSpec[conductor.PublicationMessage]{
		messageTypeID: conductor.AddPublication,
		recordLength:  ringRecordLength(conductor.PublicationMessageBodyLength(want.Channel)),
		decodeMatch: func(
			buffer aeronatomic.AtomicBuffer,
			payloadOffset, bodyLength int,
		) (conductor.PublicationMessage, bool, error) {
			return decodeMatchingAddPublication(buffer, payloadOffset, bodyLength, want)
		},
		clearedValue: func() conductor.PublicationMessage {
			return want
		},
	}
}

func findPendingAddPublication(
	ring *ringbuffer.ManyToOneRingBuffer,
	want conductor.PublicationMessage,
) (conductor.PublicationMessage, bool, error) {
	return findPendingOnRing(ring, addPublicationObserveSpec(want))
}

func findOfferedAddPublicationAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	producerBefore int64,
	want conductor.PublicationMessage,
) (conductor.PublicationMessage, bool, error) {
	return findOfferedAt(ring, producerBefore, addPublicationObserveSpec(want), nil)
}

func observeAddPublication(
	ring *ringbuffer.ManyToOneRingBuffer,
	want conductor.PublicationMessage,
	consumerBefore, producerBefore int64,
	provenPosition *int64,
) (conductor.PublicationMessage, bool, error) {
	return observeOnRing(
		ring,
		consumerBefore,
		producerBefore,
		provenPosition,
		addPublicationObserveSpec(want),
	)
}

func findConsumedAddPublicationAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	want conductor.PublicationMessage,
	consumerBefore, consumerAfter int64,
	producerBefore, producerAfter int64,
	provenPosition int64,
	onlyAddPublicationOfferedSinceSnapshot bool,
) (conductor.PublicationMessage, bool, error) {
	return findConsumedAt(
		ring,
		consumerBefore,
		consumerAfter,
		producerBefore,
		producerAfter,
		provenPosition,
		onlyAddPublicationOfferedSinceSnapshot,
		addPublicationObserveSpec(want),
	)
}

func decodeMatchingRemovePublication(
	buffer aeronatomic.AtomicBuffer,
	payloadOffset int,
	bodyLength int,
	clientID, registrationID int64,
) (conductor.RemovePublicationMessage, bool, error) {
	decoded, err := conductor.DecodeRemovePublicationMessage(buffer, payloadOffset, bodyLength)
	if err != nil {
		return conductor.RemovePublicationMessage{}, false, err
	}
	if decoded.ClientID != clientID || decoded.RegistrationID != registrationID {
		return conductor.RemovePublicationMessage{}, false, nil
	}

	return decoded, true, nil
}

func decodeMatchingRemoveSubscription(
	buffer aeronatomic.AtomicBuffer,
	payloadOffset int,
	bodyLength int,
	clientID, registrationID int64,
) (conductor.RemoveMessage, bool, error) {
	decoded, err := conductor.DecodeRemoveMessage(buffer, payloadOffset, bodyLength)
	if err != nil {
		return conductor.RemoveMessage{}, false, err
	}
	if decoded.ClientID != clientID || decoded.RegistrationID != registrationID {
		return conductor.RemoveMessage{}, false, nil
	}

	return decoded, true, nil
}

func syntheticRemovePublication(clientID, registrationID int64) conductor.RemovePublicationMessage {
	return conductor.RemovePublicationMessage{
		RemoveMessage: syntheticRemoveSubscription(clientID, registrationID),
	}
}

func syntheticRemoveSubscription(clientID, registrationID int64) conductor.RemoveMessage {
	return conductor.RemoveMessage{
		ClientID:       clientID,
		RegistrationID: registrationID,
	}
}

func removePublicationObserveSpec(
	clientID, registrationID int64,
) toDriverObserveSpec[conductor.RemovePublicationMessage] {
	return toDriverObserveSpec[conductor.RemovePublicationMessage]{
		messageTypeID: conductor.RemovePublication,
		recordLength:  ringRecordLength(conductor.RemovePublicationBodyLength),
		decodeMatch: func(
			buffer aeronatomic.AtomicBuffer,
			payloadOffset, bodyLength int,
		) (conductor.RemovePublicationMessage, bool, error) {
			return decodeMatchingRemovePublication(
				buffer,
				payloadOffset,
				bodyLength,
				clientID,
				registrationID,
			)
		},
		clearedValue: func() conductor.RemovePublicationMessage {
			return syntheticRemovePublication(clientID, registrationID)
		},
	}
}

func removeSubscriptionObserveSpec(
	clientID, registrationID int64,
) toDriverObserveSpec[conductor.RemoveMessage] {
	return toDriverObserveSpec[conductor.RemoveMessage]{
		messageTypeID: conductor.RemoveSubscription,
		recordLength:  ringRecordLength(conductor.RemoveMessageBodyLength),
		decodeMatch: func(
			buffer aeronatomic.AtomicBuffer,
			payloadOffset, bodyLength int,
		) (conductor.RemoveMessage, bool, error) {
			return decodeMatchingRemoveSubscription(
				buffer,
				payloadOffset,
				bodyLength,
				clientID,
				registrationID,
			)
		},
		clearedValue: func() conductor.RemoveMessage {
			return syntheticRemoveSubscription(clientID, registrationID)
		},
	}
}

func findPendingRemovePublication(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
) (conductor.RemovePublicationMessage, bool, error) {
	return findPendingOnRing(ring, removePublicationObserveSpec(clientID, registrationID))
}

func findOfferedRemovePublicationAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
	producerBefore int64,
) (conductor.RemovePublicationMessage, bool, error) {
	spec := removePublicationObserveSpec(clientID, registrationID)
	return findOfferedAt(ring, producerBefore, spec, nil)
}

func findConsumedRemovePublicationAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
	consumerBefore, consumerAfter int64,
	producerBefore, producerAfter int64,
	provenPosition int64,
	onlyRemovePublicationOfferedSinceSnapshot bool,
) (conductor.RemovePublicationMessage, bool, error) {
	return findConsumedAt(
		ring,
		consumerBefore,
		consumerAfter,
		producerBefore,
		producerAfter,
		provenPosition,
		onlyRemovePublicationOfferedSinceSnapshot,
		removePublicationObserveSpec(clientID, registrationID),
	)
}

func findPendingRemoveSubscription(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
) (conductor.RemoveMessage, bool, error) {
	return findPendingOnRing(ring, removeSubscriptionObserveSpec(clientID, registrationID))
}

func findOfferedRemoveSubscriptionAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
	producerBefore int64,
) (conductor.RemoveMessage, bool, error) {
	spec := removeSubscriptionObserveSpec(clientID, registrationID)
	return findOfferedAt(ring, producerBefore, spec, nil)
}

func findConsumedRemoveSubscriptionAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, registrationID int64,
	consumerBefore, consumerAfter int64,
	producerBefore, producerAfter int64,
	provenPosition int64,
	onlyRemoveSubscriptionOfferedSinceSnapshot bool,
) (conductor.RemoveMessage, bool, error) {
	return findConsumedAt(
		ring,
		consumerBefore,
		consumerAfter,
		producerBefore,
		producerAfter,
		provenPosition,
		onlyRemoveSubscriptionOfferedSinceSnapshot,
		removeSubscriptionObserveSpec(clientID, registrationID),
	)
}

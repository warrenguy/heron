//go:build !integration

package harness

import (
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

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

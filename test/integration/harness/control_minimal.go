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

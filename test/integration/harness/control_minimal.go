package harness

import (
	"fmt"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/record"
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

func findPendingClientClose(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
) (conductor.CorrelatedMessage, bool, error) {
	buffer := ring.Buffer()
	capacity := ring.Capacity()
	mask := capacity - 1

	head := ring.ConsumerPosition()
	tail := ring.ProducerPosition()
	if head >= tail {
		return conductor.CorrelatedMessage{}, false, nil
	}

	position := head
	for position < tail {
		recordIndex := int(position) & mask
		recordLength := int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
		if recordLength <= 0 {
			// Driver may be mid-consume; stop scanning like the ring consumer.
			break
		}

		alignedLength := record.AlignRecordLength(recordLength)
		messageTypeID := int(buffer.GetInt32(record.TypeOffset(recordIndex)))
		if messageTypeID == conductor.ClientClose {
			payloadOffset := record.EncodedMsgOffset(recordIndex)
			bodyLength := recordLength - ringbuffer.HeaderLength
			decoded, ok, err := decodeMatchingClientClose(
				buffer,
				payloadOffset,
				bodyLength,
				clientID,
			)
			if err != nil {
				return conductor.CorrelatedMessage{}, false, err
			}
			if ok {
				return decoded, true, nil
			}
		}

		position += int64(alignedLength)
	}

	return conductor.CorrelatedMessage{}, false, nil
}

func findOfferedClientCloseAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
	producerBefore int64,
) (conductor.CorrelatedMessage, bool, error) {
	if ring.ProducerPosition() <= producerBefore {
		return conductor.CorrelatedMessage{}, false, nil
	}

	buffer := ring.Buffer()
	capacity := ring.Capacity()
	recordIndex := int(producerBefore) & (capacity - 1)

	recordLength := int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
	if recordLength <= 0 {
		return conductor.CorrelatedMessage{}, false, nil
	}

	alignedLength := record.AlignRecordLength(recordLength)
	if int64(alignedLength) != int64(conductor.CorrelatedRingRecordLength()) {
		return conductor.CorrelatedMessage{}, false, nil
	}

	messageTypeID := int(buffer.GetInt32(record.TypeOffset(recordIndex)))
	if messageTypeID != conductor.ClientClose {
		return conductor.CorrelatedMessage{}, false, nil
	}

	payloadOffset := record.EncodedMsgOffset(recordIndex)
	bodyLength := recordLength - ringbuffer.HeaderLength
	return decodeMatchingClientClose(buffer, payloadOffset, bodyLength, clientID)
}

func observeClientClose(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
	consumerBefore, producerBefore int64,
	provenClientClosePosition *int64,
) (conductor.CorrelatedMessage, bool, error) {
	if *provenClientClosePosition < 0 {
		decoded, ok, err := findOfferedClientCloseAt(ring, clientID, producerBefore)
		if err != nil {
			return conductor.CorrelatedMessage{}, false, err
		}
		if ok {
			*provenClientClosePosition = producerBefore
			return decoded, true, nil
		}
	}

	decoded, ok, err := findPendingClientClose(ring, clientID)
	if err != nil {
		return conductor.CorrelatedMessage{}, false, err
	}
	if ok {
		if *provenClientClosePosition < 0 {
			*provenClientClosePosition = ring.ConsumerPosition()
		}
		return decoded, true, nil
	}

	return findConsumedClientCloseAt(
		ring,
		clientID,
		consumerBefore,
		ring.ConsumerPosition(),
		producerBefore,
		ring.ProducerPosition(),
		*provenClientClosePosition,
	)
}

func findConsumedClientCloseAt(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
	consumerBefore, consumerAfter int64,
	producerBefore, producerAfter int64,
	provenClientClosePosition int64,
) (conductor.CorrelatedMessage, bool, error) {
	wantRecordLength := int64(conductor.CorrelatedRingRecordLength())
	producerDelta := producerAfter - producerBefore
	consumerDelta := consumerAfter - consumerBefore
	if producerDelta != wantRecordLength || consumerDelta != wantRecordLength {
		return conductor.CorrelatedMessage{}, false, nil
	}

	buffer := ring.Buffer()
	capacity := ring.Capacity()
	recordIndex := int(consumerBefore) & (capacity - 1)

	recordLength := int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
	if recordLength <= 0 {
		if provenClientClosePosition != consumerBefore {
			return conductor.CorrelatedMessage{}, false, nil
		}

		return conductor.CorrelatedMessage{
			ClientID:      clientID,
			CorrelationID: conductor.NullCorrelationID,
		}, true, nil
	}

	alignedLength := record.AlignRecordLength(recordLength)
	if int64(alignedLength) != wantRecordLength {
		return conductor.CorrelatedMessage{}, false, nil
	}

	messageTypeID := int(buffer.GetInt32(record.TypeOffset(recordIndex)))
	if messageTypeID != conductor.ClientClose {
		return conductor.CorrelatedMessage{}, false, nil
	}

	payloadOffset := record.EncodedMsgOffset(recordIndex)
	bodyLength := recordLength - ringbuffer.HeaderLength
	return decodeMatchingClientClose(buffer, payloadOffset, bodyLength, clientID)
}

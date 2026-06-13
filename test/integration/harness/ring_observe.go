package harness

import (
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

func ringRecordLength(bodyLength int) int {
	return record.AlignRecordLength(bodyLength + ringbuffer.HeaderLength)
}

type toDriverObserveSpec[T any] struct {
	messageTypeID int
	recordLength  int
	decodeMatch   func(
		buffer aeronatomic.AtomicBuffer,
		payloadOffset, bodyLength int,
	) (T, bool, error)
	clearedValue func() T
}

func advanceRingScanPosition(messageTypeID, recordLength int) int64 {
	if messageTypeID == ringbuffer.PaddingMsgTypeID {
		return int64(recordLength)
	}

	return int64(record.AlignRecordLength(recordLength))
}

func nextRingRecord(
	buffer aeronatomic.AtomicBuffer,
	mask int,
	position int64,
) (
	recordIndex int,
	messageTypeID int,
	recordLength int,
	nextPosition int64,
	incomplete bool,
) {
	recordIndex = int(position) & mask
	recordLength = int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
	if recordLength <= 0 {
		return recordIndex, 0, 0, position, true
	}

	messageTypeID = int(buffer.GetInt32(record.TypeOffset(recordIndex)))
	nextPosition = position + advanceRingScanPosition(messageTypeID, recordLength)
	return recordIndex, messageTypeID, recordLength, nextPosition, false
}

func findPendingOnRing[T any](
	ring *ringbuffer.ManyToOneRingBuffer,
	spec toDriverObserveSpec[T],
) (T, bool, error) {
	var zero T

	buffer := ring.Buffer()
	mask := ring.Capacity() - 1

	head := ring.ConsumerPosition()
	tail := ring.ProducerPosition()
	if head >= tail {
		return zero, false, nil
	}

	for position := head; position < tail; {
		_, messageTypeID, recordLength, nextPosition, incomplete := nextRingRecord(
			buffer,
			mask,
			position,
		)
		if incomplete {
			break
		}

		if messageTypeID == spec.messageTypeID &&
			int64(record.AlignRecordLength(recordLength)) == int64(spec.recordLength) {
			recordIndex := int(position) & mask
			payloadOffset := record.EncodedMsgOffset(recordIndex)
			bodyLength := recordLength - ringbuffer.HeaderLength
			decoded, ok, err := spec.decodeMatch(buffer, payloadOffset, bodyLength)
			if err != nil {
				return zero, false, err
			}
			if ok {
				return decoded, true, nil
			}
		}

		position = nextPosition
	}

	return zero, false, nil
}

func findOfferedAt[T any](
	ring *ringbuffer.ManyToOneRingBuffer,
	producerBefore int64,
	spec toDriverObserveSpec[T],
	matchedPosition *int64,
) (T, bool, error) {
	return findOfferedBetween(
		ring,
		producerBefore,
		ring.ProducerPosition(),
		spec,
		matchedPosition,
	)
}

func findOfferedBetween[T any](
	ring *ringbuffer.ManyToOneRingBuffer,
	fromPosition, toPosition int64,
	spec toDriverObserveSpec[T],
	matchedPosition *int64,
) (T, bool, error) {
	var zero T

	if toPosition <= fromPosition {
		return zero, false, nil
	}

	buffer := ring.Buffer()
	mask := ring.Capacity() - 1

	for position := fromPosition; position < toPosition; {
		_, messageTypeID, recordLength, nextPosition, incomplete := nextRingRecord(
			buffer,
			mask,
			position,
		)
		if incomplete {
			break
		}

		if messageTypeID == spec.messageTypeID &&
			int64(record.AlignRecordLength(recordLength)) == int64(spec.recordLength) {
			recordIndex := int(position) & mask
			payloadOffset := record.EncodedMsgOffset(recordIndex)
			bodyLength := recordLength - ringbuffer.HeaderLength
			decoded, ok, err := spec.decodeMatch(buffer, payloadOffset, bodyLength)
			if err != nil {
				return zero, false, err
			}
			if ok {
				if matchedPosition != nil {
					*matchedPosition = position
				}
				return decoded, true, nil
			}
		}

		position = nextPosition
	}

	return zero, false, nil
}

func findConsumedAt[T any](
	ring *ringbuffer.ManyToOneRingBuffer,
	consumerBefore, consumerAfter int64,
	producerBefore, producerAfter int64,
	provenPosition int64,
	onlyOfferedSinceSnapshot bool,
	spec toDriverObserveSpec[T],
) (T, bool, error) {
	var zero T

	wantRecordLength := int64(spec.recordLength)
	producerDelta := producerAfter - producerBefore
	consumerDelta := consumerAfter - consumerBefore
	if producerDelta != consumerDelta {
		return zero, false, nil
	}

	onlySinceSnapshot := onlyOfferedSinceSnapshot && producerBefore == consumerBefore
	buffer := ring.Buffer()
	mask := ring.Capacity() - 1

	if producerDelta == wantRecordLength {
		recordIndex := int(consumerBefore) & mask
		recordLength := int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
		if recordLength <= 0 {
			if provenPosition == consumerBefore || onlySinceSnapshot {
				return spec.clearedValue(), true, nil
			}

			return zero, false, nil
		}

		alignedLength := record.AlignRecordLength(recordLength)
		if int64(alignedLength) != wantRecordLength {
			return zero, false, nil
		}

		messageTypeID := int(buffer.GetInt32(record.TypeOffset(recordIndex)))
		if messageTypeID != spec.messageTypeID {
			return zero, false, nil
		}

		payloadOffset := record.EncodedMsgOffset(recordIndex)
		bodyLength := recordLength - ringbuffer.HeaderLength
		return spec.decodeMatch(buffer, payloadOffset, bodyLength)
	}

	if producerDelta > wantRecordLength {
		var (
			match   T
			matched bool
		)

		for position := consumerBefore; position < consumerAfter; {
			_, messageTypeID, recordLength, nextPosition, incomplete := nextRingRecord(
				buffer,
				mask,
				position,
			)
			if incomplete {
				if provenPosition == consumerBefore || onlySinceSnapshot {
					return spec.clearedValue(), true, nil
				}

				break
			}

			if messageTypeID == ringbuffer.PaddingMsgTypeID {
				position = nextPosition
				continue
			}

			if messageTypeID != spec.messageTypeID {
				return zero, false, nil
			}

			alignedLength := record.AlignRecordLength(recordLength)
			if int64(alignedLength) != wantRecordLength {
				return zero, false, nil
			}

			recordIndex := int(position) & mask
			payloadOffset := record.EncodedMsgOffset(recordIndex)
			bodyLength := recordLength - ringbuffer.HeaderLength
			decoded, ok, err := spec.decodeMatch(buffer, payloadOffset, bodyLength)
			if err != nil {
				return zero, false, err
			}
			if !ok {
				return zero, false, nil
			}

			if matched {
				return zero, false, nil
			}

			match = decoded
			matched = true
			position = nextPosition
		}

		if matched {
			return match, true, nil
		}

		return zero, false, nil
	}

	return zero, false, nil
}

func observeOnRing[T any](
	ring *ringbuffer.ManyToOneRingBuffer,
	consumerBefore, producerBefore int64,
	provenPosition *int64,
	spec toDriverObserveSpec[T],
) (T, bool, error) {
	var zero T

	if *provenPosition < 0 {
		var matchedPosition int64
		decoded, ok, err := findOfferedAt(ring, producerBefore, spec, &matchedPosition)
		if err != nil {
			return zero, false, err
		}
		if ok {
			*provenPosition = matchedPosition
			return decoded, true, nil
		}
	}

	decoded, ok, err := findPendingOnRing(ring, spec)
	if err != nil {
		return zero, false, err
	}
	if ok {
		if *provenPosition < 0 {
			*provenPosition = ring.ConsumerPosition()
		}
		return decoded, true, nil
	}

	consumerAfter := ring.ConsumerPosition()
	producerAfter := ring.ProducerPosition()
	return findConsumedAt(
		ring,
		consumerBefore,
		consumerAfter,
		producerBefore,
		producerAfter,
		*provenPosition,
		producerBefore == consumerBefore,
		spec,
	)
}

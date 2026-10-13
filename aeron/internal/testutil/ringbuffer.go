package testutil

import (
	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/record"
	"github.com/warrenguy/heron/ringbuffer"
)

func visitCommittedNonPaddingRecords(
	ring *ringbuffer.ManyToOneRingBuffer,
	visitor func(messageTypeID int),
) {
	buffer := ring.Buffer()
	head := ring.ConsumerPosition()
	tail := ring.ProducerPosition()
	if head >= tail {
		return
	}

	capacity := ring.Capacity()
	mask := capacity - 1
	position := head

	for position < tail {
		headIndex := int(position) & mask
		bytesRead := 0
		maxBlockLength := capacity - headIndex

		for bytesRead < maxBlockLength && position < tail {
			recordIndex := headIndex + bytesRead
			recordLength := int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
			if recordLength <= 0 {
				return
			}

			alignedLength := record.AlignRecordLength(recordLength)
			bytesRead += alignedLength
			position += int64(alignedLength)

			messageTypeID := int(buffer.GetInt32(record.TypeOffset(recordIndex)))
			if messageTypeID != ringbuffer.PaddingMsgTypeID {
				visitor(messageTypeID)
			}
		}
	}
}

// CommittedNonPaddingRecordCount walks committed ring records from consumer to
// producer and counts those that are not padding.
func CommittedNonPaddingRecordCount(ring *ringbuffer.ManyToOneRingBuffer) int {
	count := 0
	visitCommittedNonPaddingRecords(ring, func(int) { count++ })
	return count
}

// CommittedRecordTypeIDs returns committed non-padding message type ids in order.
func CommittedRecordTypeIDs(ring *ringbuffer.ManyToOneRingBuffer) []int {
	typeIDs := make([]int, 0, 4)
	visitCommittedNonPaddingRecords(ring, func(messageTypeID int) {
		typeIDs = append(typeIDs, messageTypeID)
	})
	return typeIDs
}

// drainCommittedRecords reads until the consumer position stops advancing.
// One Read stops at the buffer end, so records past a wrap need a second Read.
// A padding-only block advances the consumer position but reads zero messages.
func drainCommittedRecords(
	ring *ringbuffer.ManyToOneRingBuffer,
	handler ringbuffer.MessageHandler,
) {
	for {
		before := ring.ConsumerPosition()
		ring.Read(handler)
		if ring.ConsumerPosition() == before {
			return
		}
	}
}

// RemovePublicationRegistrationIDs consumes every committed ring record and
// returns the registration ids of REMOVE_PUBLICATION commands in order. Other
// command types are consumed and skipped.
func RemovePublicationRegistrationIDs(
	t require.TestingT,
	ring *ringbuffer.ManyToOneRingBuffer,
) []int64 {
	registrationIDs := make([]int64, 0, 1)
	drainCommittedRecords(ring, func(
		msgTypeID int,
		buffer atomicbuffer.AtomicBuffer,
		index, length int,
	) {
		if msgTypeID != conductor.RemovePublication {
			return
		}
		message, err := conductor.DecodeRemovePublicationMessage(buffer, index, length)
		require.NoError(t, err)
		registrationIDs = append(registrationIDs, message.RegistrationID)
	})
	return registrationIDs
}

// AddPublicationCorrelationIDs consumes every committed ring record and
// returns the correlation ids of ADD_PUBLICATION and
// ADD_EXCLUSIVE_PUBLICATION commands in order. Other command types are
// consumed and skipped.
func AddPublicationCorrelationIDs(
	t require.TestingT,
	ring *ringbuffer.ManyToOneRingBuffer,
) []int64 {
	correlationIDs := make([]int64, 0, 2)
	drainCommittedRecords(ring, func(
		msgTypeID int,
		buffer atomicbuffer.AtomicBuffer,
		index, length int,
	) {
		if msgTypeID != conductor.AddPublication && msgTypeID != conductor.AddExclusivePublication {
			return
		}
		message, err := conductor.DecodePublicationMessage(buffer, index, length)
		require.NoError(t, err)
		correlationIDs = append(correlationIDs, message.CorrelationID)
	})
	return correlationIDs
}

package testutil

import (
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

// CommittedNonPaddingRecordCount walks committed ring records from consumer to
// producer and counts those that are not padding.
func CommittedNonPaddingRecordCount(ring *ringbuffer.ManyToOneRingBuffer) int {
	buffer := ring.Buffer()
	head := ring.ConsumerPosition()
	tail := ring.ProducerPosition()
	if head >= tail {
		return 0
	}

	capacity := ring.Capacity()
	mask := capacity - 1
	count := 0
	position := head

	for position < tail {
		headIndex := int(position) & mask
		bytesRead := 0
		maxBlockLength := capacity - headIndex

		for bytesRead < maxBlockLength && position < tail {
			recordIndex := headIndex + bytesRead
			recordLength := int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
			if recordLength <= 0 {
				return count
			}

			alignedLength := record.AlignRecordLength(recordLength)
			bytesRead += alignedLength
			position += int64(alignedLength)

			messageTypeID := int(buffer.GetInt32(record.TypeOffset(recordIndex)))
			if messageTypeID != ringbuffer.PaddingMsgTypeID {
				count++
			}
		}
	}

	return count
}

package broadcast

import (
	"fmt"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/record"
)

type BroadcastTransmitter struct {
	buffer           atomic.AtomicBuffer
	capacity         int
	maxMsgLength     int
	tailIntentIndex  int
	tailCounterIndex int
	latestIndex      int
}

// NewBroadcastTransmitter constructs a transmitter over buffer. Transmit is
// single-writer; it is not safe for concurrent Transmit calls.
func NewBroadcastTransmitter(buffer atomic.AtomicBuffer) (*BroadcastTransmitter, error) {
	capacity, err := CheckCapacity(buffer.Capacity())
	if err != nil {
		return nil, err
	}
	buffer.VerifyAlignment(0, Alignment)
	if err := atomic.ValidateBackingSlice(buffer.Bytes(), Alignment); err != nil {
		return nil, err
	}

	return &BroadcastTransmitter{
		buffer:           buffer,
		capacity:         capacity,
		maxMsgLength:     MaxMsgLength(capacity),
		tailIntentIndex:  TailIntentIndex(capacity),
		tailCounterIndex: TailIndex(capacity),
		latestIndex:      LatestIndex(capacity),
	}, nil
}

func (t *BroadcastTransmitter) Capacity() int {
	return t.capacity
}

func (t *BroadcastTransmitter) MaxMsgLength() int {
	return t.maxMsgLength
}

func (t *BroadcastTransmitter) Transmit(msgTypeID int, src []byte) error {
	if err := record.CheckTypeID(msgTypeID); err != nil {
		return err
	}
	if len(src) > t.maxMsgLength {
		return fmt.Errorf("broadcast: encoded message exceeds maxMsgLength of %d, length=%d",
			t.maxMsgLength, len(src))
	}

	currentTail := t.buffer.GetInt64Volatile(t.tailCounterIndex)
	recordOffset := int(currentTail) & (t.capacity - 1)
	recordLength := HeaderLength + len(src)
	recordLengthAligned := record.AlignRecordLength(recordLength)
	if recordLengthAligned > t.capacity {
		return fmt.Errorf("broadcast: aligned record length %d exceeds capacity %d",
			recordLengthAligned, t.capacity)
	}
	newTail := currentTail + int64(recordLengthAligned)

	toEndOfBuffer := t.capacity - recordOffset
	if toEndOfBuffer < recordLengthAligned {
		t.signalTailIntent(newTail + int64(toEndOfBuffer))
		atomic.ReleaseFence()
		t.insertPaddingRecord(recordOffset, toEndOfBuffer)

		currentTail += int64(toEndOfBuffer)
		recordOffset = 0
	} else {
		t.signalTailIntent(newTail)
		atomic.ReleaseFence()
	}

	t.buffer.PutInt32(record.LengthOffset(recordOffset), int32(recordLength))
	t.buffer.PutInt32(record.TypeOffset(recordOffset), int32(msgTypeID))
	t.buffer.PutBytes(record.EncodedMsgOffset(recordOffset), src)

	t.buffer.PutInt64Release(t.latestIndex, currentTail)
	t.buffer.PutInt64Release(t.tailCounterIndex, currentTail+int64(recordLengthAligned))

	return nil
}

func (t *BroadcastTransmitter) signalTailIntent(newTail int64) {
	t.buffer.PutInt64Release(t.tailIntentIndex, newTail)
}

func (t *BroadcastTransmitter) insertPaddingRecord(recordOffset, length int) {
	t.buffer.PutInt32(record.LengthOffset(recordOffset), int32(length))
	t.buffer.PutInt32(record.TypeOffset(recordOffset), PaddingMsgTypeID)
}

// Buffer returns the underlying atomic buffer (symmetry with BroadcastReceiver).
func (t *BroadcastTransmitter) Buffer() atomic.AtomicBuffer {
	return t.buffer
}

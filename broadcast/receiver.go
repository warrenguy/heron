// Package broadcast implements Agrona broadcast buffer wire layout.
//
// Buffers match the Agrona/C driver broadcast descriptor layout for driver and
// CnC interop. Trailer and record layout are fixed; not a generic pub/sub queue.
//
// Transmit is single-writer; ReceiveNext is single-consumer (not concurrent).
package broadcast

import (
	"sync/atomic"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/record"
)

type BroadcastReceiver struct {
	buffer           atomicbuffer.AtomicBuffer
	capacity         int
	tailIntentIndex  int
	tailCounterIndex int
	latestIndex      int

	cursor       int64
	nextRecord   int64
	recordOffset int
	// lapped reports that the last ReceiveNext counted a lap.
	lapped bool

	lappedCount atomic.Int64
}

// NewBroadcastReceiver constructs a receiver over buffer.
//
// ReceiveNext is not safe for concurrent use.
func NewBroadcastReceiver(buffer atomicbuffer.AtomicBuffer) (*BroadcastReceiver, error) {
	capacity, err := CheckCapacity(buffer.Capacity())
	if err != nil {
		return nil, err
	}
	buffer.VerifyAlignment(0, Alignment)
	if err := atomicbuffer.ValidateBackingSlice(buffer.Bytes(), Alignment); err != nil {
		return nil, err
	}

	cursor := buffer.GetInt64Volatile(LatestIndex(capacity))

	return &BroadcastReceiver{
		buffer:           buffer,
		capacity:         capacity,
		tailIntentIndex:  TailIntentIndex(capacity),
		tailCounterIndex: TailIndex(capacity),
		latestIndex:      LatestIndex(capacity),
		cursor:           cursor,
		nextRecord:       cursor,
		recordOffset:     maskedRecordOffset(cursor, capacity),
	}, nil
}

func (r *BroadcastReceiver) Capacity() int {
	return r.capacity
}

func (r *BroadcastReceiver) LappedCount() int64 {
	return r.lappedCount.Load()
}

func (r *BroadcastReceiver) TypeID() int {
	return int(r.buffer.GetInt32Volatile(record.TypeOffset(r.recordOffset)))
}

func (r *BroadcastReceiver) Offset() int {
	return record.EncodedMsgOffset(r.recordOffset)
}

// Length returns the body length of the record from ReceiveNext.
//
// A record length below HeaderLength yields a negative value. No conforming
// transmitter writes one.
func (r *BroadcastReceiver) Length() int {
	return int(r.buffer.GetInt32Volatile(record.LengthOffset(r.recordOffset))) - HeaderLength
}

func (r *BroadcastReceiver) Buffer() atomicbuffer.AtomicBuffer {
	return r.buffer
}

func (r *BroadcastReceiver) pendingRecordAt(
	cursor int64,
) (recordOffset, recordLength, msgTypeID int, ok bool) {
	tail := r.buffer.GetInt64Volatile(r.tailCounterIndex)
	if tail <= cursor {
		return 0, 0, 0, false
	}

	atomicbuffer.AcquireFence()

	recordOffset = maskedRecordOffset(cursor, r.capacity)
	if !r.recordValid(cursor, r.buffer.GetInt64Volatile(r.tailIntentIndex)) {
		return 0, 0, 0, false
	}

	recordLength = int(r.buffer.GetInt32Volatile(record.LengthOffset(recordOffset)))
	msgTypeID = int(r.buffer.GetInt32Volatile(record.TypeOffset(recordOffset)))
	if msgTypeID == PaddingMsgTypeID {
		cursor += int64(record.AlignRecordLength(recordLength))
		if tail <= cursor {
			return 0, 0, 0, false
		}

		recordOffset = maskedRecordOffset(cursor, r.capacity)
		recordLength = int(r.buffer.GetInt32Volatile(record.LengthOffset(recordOffset)))
		msgTypeID = int(r.buffer.GetInt32Volatile(record.TypeOffset(recordOffset)))
	}

	if msgTypeID == PaddingMsgTypeID || recordLength < HeaderLength {
		return 0, 0, 0, false
	}

	return recordOffset, recordLength, msgTypeID, true
}

func (r *BroadcastReceiver) ReceiveNext() bool {
	tail := r.buffer.GetInt64Volatile(r.tailCounterIndex)
	cursor := r.nextRecord

	if tail <= cursor {
		r.lapped = false
		return false
	}

	atomicbuffer.AcquireFence()

	recordOffset := maskedRecordOffset(cursor, r.capacity)

	lapped := !r.recordValid(cursor, r.buffer.GetInt64Volatile(r.tailIntentIndex))
	if lapped {
		r.lappedCount.Add(1)

		cursor = r.buffer.GetInt64Volatile(r.latestIndex)
		recordOffset = maskedRecordOffset(cursor, r.capacity)
		atomicbuffer.AcquireFence()
	}

	r.lapped = lapped
	r.cursor = cursor
	recordLength := int(r.buffer.GetInt32Volatile(record.LengthOffset(recordOffset)))
	r.nextRecord = cursor + int64(record.AlignRecordLength(recordLength))

	// cursor stays on the padding slot, so Validate covers the padding length.
	if int(r.buffer.GetInt32Volatile(record.TypeOffset(recordOffset))) == PaddingMsgTypeID {
		recordOffset = 0
		wrappedRecordLength := int(r.buffer.GetInt32Volatile(record.LengthOffset(recordOffset)))
		r.nextRecord += int64(record.AlignRecordLength(wrappedRecordLength))
	}

	r.recordOffset = recordOffset

	return true
}

// Validate reports whether the record from ReceiveNext is still intact.
// Read TypeID, Length, and the payload before the call. Trust the reads only
// when Validate reports true.
//
// The check covers every length that ReceiveNext loaded, a padding length
// included.
func (r *BroadcastReceiver) Validate() bool {
	atomicbuffer.AcquireFence()
	return r.recordValid(r.cursor, r.buffer.GetInt64Acquire(r.tailIntentIndex))
}

// Resync moves the next ReceiveNext to the latest record. It counts a lap
// unless the last ReceiveNext counted one.
//
// Call Resync after Validate reports false. A torn length can leave the next
// record off a record boundary. The latest position is always a record start.
func (r *BroadcastReceiver) Resync() {
	if !r.lapped {
		r.lappedCount.Add(1)
	}
	r.lapped = false
	r.nextRecord = r.buffer.GetInt64Volatile(r.latestIndex)
}

func (r *BroadcastReceiver) recordValid(cursor, tailIntent int64) bool {
	return cursor+int64(r.capacity) > tailIntent
}

func maskedRecordOffset(cursor int64, capacity int) int {
	return int(cursor) & (capacity - 1)
}

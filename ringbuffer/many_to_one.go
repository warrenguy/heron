// Package ringbuffer implements Agrona ManyToOneRingBuffer wire layout.
//
// Buffers match the Agrona/C driver memory layout for CnC and to-driver ring
// interop. Record headers and trailer counters are fixed; not a generic MPSC queue.
//
// Many producers may claim/write concurrently; one consumer goroutine drives Read.
package ringbuffer

import (
	"errors"
	"fmt"
	"math"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/record"
)

type MessageHandler func(
	msgTypeID int,
	buffer atomicbuffer.AtomicBuffer,
	index int,
	length int,
)

// ControlledAction is the ordinal home for controlled consumer position actions.
//
// Same ordinals as Agrona ControlledMessageHandler / Aeron ControlledFragmentHandler.
//
// Package aeron re-exports these for Image controlled poll and cluster egress.
// Ring [ControlledRead] uses this type directly.
//
// Values: [ControlledAbort], [ControlledBreak], [ControlledCommit],
// [ControlledContinue].
type ControlledAction int

const (
	// ControlledAbort stops without advancing past the current item.
	ControlledAbort ControlledAction = iota
	// ControlledBreak stops after committing the current item.
	ControlledBreak
	// ControlledCommit publishes progress through the current item and continues.
	ControlledCommit
	// ControlledContinue continues; progress commits at consumer end or after Commit.
	ControlledContinue
)

// ControlledMessageHandler is a ring Read handler that returns [ControlledAction].
type ControlledMessageHandler func(
	msgTypeID int,
	buffer atomicbuffer.AtomicBuffer,
	index int,
	length int,
) ControlledAction

type ManyToOneRingBuffer struct {
	capacity                  int
	maxMsgLength              int
	tailPositionIndex         int
	headCachePositionIndex    int
	headPositionIndex         int
	correlationIDCounterIndex int
	consumerHeartbeatIndex    int
	buffer                    atomicbuffer.AtomicBuffer
}

func NewManyToOneRingBuffer(buffer atomicbuffer.AtomicBuffer) (*ManyToOneRingBuffer, error) {
	capacity, err := CheckCapacity(buffer.Capacity(), MinCapacity)
	if err != nil {
		return nil, err
	}

	buffer.VerifyAlignment(0, Alignment)

	if err := atomicbuffer.ValidateBackingSlice(buffer.Bytes(), Alignment); err != nil {
		return nil, err
	}

	maxMsgLength := MaxMsgLength(capacity)

	return &ManyToOneRingBuffer{
		capacity:                  capacity,
		maxMsgLength:              maxMsgLength,
		tailPositionIndex:         TailIndex(capacity),
		headCachePositionIndex:    HeadCacheIndex(capacity),
		headPositionIndex:         HeadIndex(capacity),
		correlationIDCounterIndex: CorrelationIndex(capacity),
		consumerHeartbeatIndex:    ConsumerHeartbeatIndex(capacity),
		buffer:                    buffer,
	}, nil
}

func (ring *ManyToOneRingBuffer) Capacity() int {
	return ring.capacity
}

func (ring *ManyToOneRingBuffer) MaxMsgLength() int {
	return ring.maxMsgLength
}

func (ring *ManyToOneRingBuffer) Buffer() atomicbuffer.AtomicBuffer {
	return ring.buffer
}

func (ring *ManyToOneRingBuffer) ProducerPosition() int64 {
	return ring.buffer.GetInt64Volatile(ring.tailPositionIndex)
}

func (ring *ManyToOneRingBuffer) ConsumerPosition() int64 {
	return ring.buffer.GetInt64Volatile(ring.headPositionIndex)
}

func (ring *ManyToOneRingBuffer) NextCorrelationID() int64 {
	return ring.buffer.GetAndAddInt64(ring.correlationIDCounterIndex, 1)
}

func (ring *ManyToOneRingBuffer) ConsumerHeartbeatTime() int64 {
	return ring.buffer.GetInt64Volatile(ring.consumerHeartbeatIndex)
}

func (ring *ManyToOneRingBuffer) SetConsumerHeartbeatTime(timeMs int64) {
	ring.buffer.PutInt64Release(ring.consumerHeartbeatIndex, timeMs)
}

func (ring *ManyToOneRingBuffer) Size() int {
	buffer := ring.buffer
	headPositionIndex := ring.headPositionIndex
	tailPositionIndex := ring.tailPositionIndex

	headAfter := buffer.GetInt64Volatile(headPositionIndex)

	for {
		headBefore := headAfter
		tail := buffer.GetInt64Volatile(tailPositionIndex)
		headAfter = buffer.GetInt64Volatile(headPositionIndex)
		if headAfter == headBefore {
			size := tail - headAfter
			if size < 0 {
				return 0
			}
			if size > int64(ring.capacity) {
				return ring.capacity
			}
			return int(size)
		}
	}
}

func (ring *ManyToOneRingBuffer) Write(
	msgTypeID int,
	srcBuffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) (bool, error) {
	recordIndex, encodedOffset, err := ring.claimRecord(msgTypeID, length)
	if err != nil {
		return false, err
	}
	if recordIndex == InsufficientCapacity {
		return false, nil
	}

	buffer := ring.buffer
	recordLength := length + HeaderLength
	buffer.PutBytesFrom(encodedOffset, srcBuffer, offset, length)
	buffer.PutInt32Release(record.LengthOffset(recordIndex), int32(recordLength))

	return true, nil
}

func (ring *ManyToOneRingBuffer) TryClaim(msgTypeID, length int) (int, error) {
	recordIndex, encodedOffset, err := ring.claimRecord(msgTypeID, length)
	if err != nil {
		return -1, err
	}
	if recordIndex == InsufficientCapacity {
		return InsufficientCapacity, nil
	}

	return encodedOffset, nil
}

func (ring *ManyToOneRingBuffer) claimRecord(
	msgTypeID int,
	length int,
) (
	recordIndex int,
	encodedOffset int,
	err error,
) {
	if err := record.CheckTypeID(msgTypeID); err != nil {
		return -1, -1, err
	}
	if err := ring.checkMsgLength(length); err != nil {
		return -1, -1, err
	}

	buffer := ring.buffer
	recordLength := length + HeaderLength
	recordIndex = ring.claimCapacity(buffer, recordLength)
	if recordIndex == InsufficientCapacity {
		return InsufficientCapacity, -1, nil
	}

	beginInFlightRecord(buffer, recordIndex, recordLength)
	buffer.PutInt32(record.TypeOffset(recordIndex), int32(msgTypeID))

	return recordIndex, record.EncodedMsgOffset(recordIndex), nil
}

func (ring *ManyToOneRingBuffer) Commit(index int) error {
	return ring.finishClaim(index, false)
}

func (ring *ManyToOneRingBuffer) Abort(index int) error {
	return ring.finishClaim(index, true)
}

func (ring *ManyToOneRingBuffer) finishClaim(index int, abort bool) error {
	recordIndex, err := ring.computeRecordIndex(index)
	if err != nil {
		return err
	}

	buffer := ring.buffer
	recordLength, err := verifyClaimedSpaceNotReleased(buffer, recordIndex)
	if err != nil {
		return err
	}

	if abort {
		buffer.PutInt32(record.TypeOffset(recordIndex), PaddingMsgTypeID)
	}
	buffer.PutInt32Release(record.LengthOffset(recordIndex), -recordLength)
	return nil
}

func (ring *ManyToOneRingBuffer) Read(handler MessageHandler) int {
	return ring.ReadWithLimit(handler, math.MaxInt32)
}

func (ring *ManyToOneRingBuffer) ReadWithLimit(handler MessageHandler, messageCountLimit int) int {
	return ring.ControlledReadWithLimit(func(
		msgTypeID int,
		buffer atomicbuffer.AtomicBuffer,
		index int,
		length int,
	) ControlledAction {
		handler(msgTypeID, buffer, index, length)
		return ControlledContinue
	}, messageCountLimit)
}

func (ring *ManyToOneRingBuffer) ControlledRead(handler ControlledMessageHandler) int {
	return ring.ControlledReadWithLimit(handler, math.MaxInt32)
}

func (ring *ManyToOneRingBuffer) ControlledReadWithLimit(
	handler ControlledMessageHandler,
	messageCountLimit int,
) int {
	if messageCountLimit <= 0 {
		return 0
	}

	messagesRead := 0

	buffer := ring.buffer
	headPositionIndex := ring.headPositionIndex
	head := buffer.GetInt64(headPositionIndex)

	capacity := ring.capacity
	headIndex := int(head) & (capacity - 1)
	maxBlockLength := capacity - headIndex
	bytesRead := 0

	defer func() {
		releaseConsumedBlock(buffer, headPositionIndex, headIndex, head, bytesRead, capacity)
	}()

	for bytesRead < maxBlockLength && messagesRead < messageCountLimit {
		recordIndex := headIndex + bytesRead
		recordLength := int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
		if recordLength <= 0 {
			break
		}

		alignedLength := record.AlignRecordLength(recordLength)
		bytesRead += alignedLength

		messageTypeID := int(buffer.GetInt32(record.TypeOffset(recordIndex)))
		if messageTypeID == PaddingMsgTypeID {
			continue
		}

		action := handler(
			messageTypeID,
			buffer,
			record.EncodedMsgOffset(recordIndex),
			recordLength-HeaderLength,
		)

		if action == ControlledAbort {
			bytesRead -= alignedLength
			break
		}

		messagesRead++

		if action == ControlledBreak {
			break
		}
		if action == ControlledCommit {
			headIndex, head = releaseConsumedBlock(
				buffer,
				headPositionIndex,
				headIndex,
				head,
				bytesRead,
				capacity,
			)
			bytesRead = 0
			maxBlockLength = capacity - headIndex
		}
	}

	return messagesRead
}

// Unblock recovers a stalled consumer on in-flight (negative length) or zeroed-gap records.
//
// Idle or recovery only: not safe while TryClaim/Write producers are active
// (races in-flight length).
func (ring *ManyToOneRingBuffer) Unblock() bool {
	buffer := ring.buffer
	headPositionIndex := ring.headPositionIndex
	tailPositionIndex := ring.tailPositionIndex

	headPosition := buffer.GetInt64Volatile(headPositionIndex)
	tailPosition := buffer.GetInt64Volatile(tailPositionIndex)

	if headPosition == tailPosition {
		return false
	}

	mask := ring.capacity - 1
	consumerIndex := int(headPosition) & mask
	producerIndex := int(tailPosition) & mask

	unblocked := false
	length := buffer.GetInt32Volatile(consumerIndex)
	if length < 0 {
		buffer.PutInt32(record.TypeOffset(consumerIndex), PaddingMsgTypeID)
		buffer.PutInt32Release(record.LengthOffset(consumerIndex), -length)
		unblocked = true
	} else if length == 0 {
		limit := producerIndex
		if producerIndex <= consumerIndex {
			limit = ring.capacity
		}

		for i := consumerIndex + Alignment; i < limit; i += Alignment {
			length = buffer.GetInt32Volatile(i)
			if length != 0 {
				if scanBackToConfirmStillZeroed(buffer, i, consumerIndex) {
					buffer.PutInt32(record.TypeOffset(consumerIndex), PaddingMsgTypeID)
					buffer.PutInt32Release(record.LengthOffset(consumerIndex), int32(i-consumerIndex))
					unblocked = true
				}
				break
			}
		}
	}

	return unblocked
}

func scanBackToConfirmStillZeroed(buffer atomicbuffer.AtomicBuffer, from, limit int) bool {
	for i := from - Alignment; i >= limit; i -= Alignment {
		if buffer.GetInt32Volatile(i) != 0 {
			return false
		}
	}
	return true
}

func (ring *ManyToOneRingBuffer) claimCapacity(
	buffer atomicbuffer.AtomicBuffer,
	recordLength int,
) int {
	requiredCapacity := record.AlignRecordLength(recordLength)
	capacity := ring.capacity
	tailPositionIndex := ring.tailPositionIndex
	headCachePositionIndex := ring.headCachePositionIndex
	headPositionIndex := ring.headPositionIndex
	mask := capacity - 1

	head := buffer.GetInt64Volatile(headCachePositionIndex)

	var (
		tail       int64
		newTail    int64
		tailIndex  int
		padding    int
		writeIndex int
	)

	for {
		tail = buffer.GetInt64Volatile(tailPositionIndex)
		availableCapacity := int64(capacity) - (tail - head)

		if int64(requiredCapacity) > availableCapacity {
			head = buffer.GetInt64Volatile(headPositionIndex)
			if int64(requiredCapacity) > int64(capacity)-(tail-head) {
				return InsufficientCapacity
			}

			buffer.PutInt64Release(headCachePositionIndex, head)
		}

		newTail = tail + int64(requiredCapacity)

		padding = 0
		tailIndex = int(tail) & mask
		writeIndex = tailIndex
		toBufferEndLength := capacity - tailIndex

		if requiredCapacity > toBufferEndLength {
			headIndex := int(head) & mask
			writeIndex = 0

			if requiredCapacity > headIndex {
				head = buffer.GetInt64Volatile(headPositionIndex)
				headIndex = int(head) & mask
				if requiredCapacity > headIndex {
					writeIndex = InsufficientCapacity
					newTail = tail
				}

				buffer.PutInt64Release(headCachePositionIndex, head)
			}

			padding = toBufferEndLength
			newTail += int64(padding)
		}

		if buffer.CompareAndSwapInt64(tailPositionIndex, tail, newTail) {
			break
		}
	}

	if padding != 0 {
		publishPaddingRecord(buffer, tailIndex, padding)
	}

	return writeIndex
}

func (ring *ManyToOneRingBuffer) computeRecordIndex(index int) (int, error) {
	recordIndex := index - HeaderLength
	if recordIndex < 0 || recordIndex > ring.capacity-HeaderLength {
		return 0, fmt.Errorf("ringbuffer: invalid message index %d", index)
	}
	return recordIndex, nil
}

func (ring *ManyToOneRingBuffer) checkMsgLength(length int) error {
	if length < 0 {
		return fmt.Errorf("ringbuffer: invalid message length=%d", length)
	}
	if length > ring.maxMsgLength {
		return fmt.Errorf(
			"ringbuffer: encoded message exceeds maxMsgLength=%d, length=%d",
			ring.maxMsgLength,
			length,
		)
	}
	return nil
}

func verifyClaimedSpaceNotReleased(
	buffer atomicbuffer.AtomicBuffer,
	recordIndex int,
) (int32, error) {
	recordLength := buffer.GetInt32Volatile(record.LengthOffset(recordIndex))
	if recordLength < 0 {
		return recordLength, nil
	}
	if recordLength == 0 {
		return 0, errors.New("ringbuffer: claimed space not in flight")
	}

	state := "committed"
	if int(buffer.GetInt32(record.TypeOffset(recordIndex))) == PaddingMsgTypeID {
		state = "aborted"
	}
	return 0, fmt.Errorf("ringbuffer: claimed space previously %s", state)
}

func releaseConsumedBlock(
	buffer atomicbuffer.AtomicBuffer,
	headPositionIndex int,
	headIndex int,
	head int64,
	bytesRead int,
	capacity int,
) (int, int64) {
	if bytesRead <= 0 {
		return headIndex, head
	}
	buffer.SetBytes(headIndex, bytesRead, 0)
	head += int64(bytesRead)
	buffer.PutInt64Release(headPositionIndex, head)
	return int(head) & (capacity - 1), head
}

func beginInFlightRecord(buffer atomicbuffer.AtomicBuffer, recordIndex, recordLength int) {
	buffer.PutInt32Release(record.LengthOffset(recordIndex), int32(-recordLength))
	atomicbuffer.ReleaseFence()
}

func publishPaddingRecord(buffer atomicbuffer.AtomicBuffer, recordIndex, length int) {
	buffer.PutInt32Release(record.LengthOffset(recordIndex), int32(-length))
	atomicbuffer.ReleaseFence()
	buffer.PutInt32(record.TypeOffset(recordIndex), PaddingMsgTypeID)
	buffer.PutInt32Release(record.LengthOffset(recordIndex), int32(length))
}

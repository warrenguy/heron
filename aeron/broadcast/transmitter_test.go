package broadcast_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/record"
)

func TestBroadcastTransmitterCapacity(t *testing.T) {
	tx := newTransmitter(t, newTestBuffer(t))
	require.Equal(t, testCapacity, tx.Capacity())
}

func TestBroadcastTransmitterInvalidCapacity(t *testing.T) {
	_, err := broadcast.NewBroadcastTransmitter(invalidCapacityBuffer())
	require.Error(t, err)
	_, err = broadcast.NewBroadcastTransmitter(
		atomic.Wrap(make([]byte, broadcast.TotalBufferLength(8))),
	)
	require.Error(t, err)
}

func TestBroadcastTransmitterMaxMessageLengthExceeded(t *testing.T) {
	tx := newTransmitter(t, newTestBuffer(t))
	src := make([]byte, tx.MaxMsgLength()+1)
	err := tx.Transmit(testMsgTypeID, src)
	require.Error(t, err)
}

func TestBroadcastTransmitterInvalidMessageTypeID(t *testing.T) {
	tx := newTransmitter(t, newTestBuffer(t))
	err := tx.Transmit(-1, make([]byte, 32))
	require.Error(t, err)
}

func TestBroadcastTransmitterIntoEmptyBuffer(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := testPayload8
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	recordLength := len(src) + broadcast.HeaderLength
	recordLengthAligned := alignedRecordLength(len(src))

	tailIntentIdx := broadcast.TailIntentIndex(testCapacity)
	tailIdx := broadcast.TailIndex(testCapacity)
	latestIdx := broadcast.LatestIndex(testCapacity)
	require.Equal(t, int64(recordLengthAligned), buf.GetInt64Volatile(tailIntentIdx))
	require.Equal(t, int64(0), buf.GetInt64Volatile(latestIdx))
	require.Equal(t, int64(recordLengthAligned), buf.GetInt64Volatile(tailIdx))
	require.Equal(t, int32(recordLength), buf.GetInt32(record.LengthOffset(0)))
	require.Equal(t, int32(testMsgTypeID), buf.GetInt32(record.TypeOffset(0)))
	got := buf.Bytes()[broadcast.MsgOffset(0) : broadcast.MsgOffset(0)+len(src)]
	require.Equal(t, src, got)
}

func TestBroadcastTransmitterIntoUsedBuffer(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	const tail = broadcast.Alignment * 3
	buf.PutInt64(broadcast.TailIndex(testCapacity), tail)

	src := []byte{9, 8, 7, 6, 5, 4, 3, 2}
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	recordLengthAligned := alignedRecordLength(len(src))
	recordOffset := int(tail)

	tailIntentIdx := broadcast.TailIntentIndex(testCapacity)
	tailIdx := broadcast.TailIndex(testCapacity)
	latestIdx := broadcast.LatestIndex(testCapacity)
	wantTail := tail + int64(recordLengthAligned)
	require.Equal(t, wantTail, buf.GetInt64Volatile(tailIntentIdx))
	require.Equal(t, int64(tail), buf.GetInt64Volatile(latestIdx))
	require.Equal(t, wantTail, buf.GetInt64Volatile(tailIdx))
	require.Equal(t, int32(testMsgTypeID), buf.GetInt32(record.TypeOffset(recordOffset)))
}

func TestBroadcastTransmitterIntoEndOfBuffer(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := []byte{1, 1, 2, 2, 3, 3, 4, 4}
	recordLengthAligned := alignedRecordLength(len(src))
	tail := int64(testCapacity - recordLengthAligned)

	buf.PutInt64(broadcast.TailIndex(testCapacity), tail)
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	recordOffset := int(tail)
	wantTail := tail + int64(recordLengthAligned)
	require.Equal(t, wantTail, buf.GetInt64Volatile(broadcast.TailIndex(testCapacity)))
	require.Equal(t, int32(testMsgTypeID), buf.GetInt32(record.TypeOffset(recordOffset)))
}

func TestBroadcastTransmitterPaddingAtEndOfBuffer(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	tail := int64(testCapacity - broadcast.Alignment)
	buf.PutInt64(broadcast.TailIndex(testCapacity), tail)

	const length = broadcast.Alignment + 8
	src := make([]byte, length)
	for i := range src {
		src[i] = byte(i)
	}
	err := tx.Transmit(testMsgTypeID, src)
	require.NoError(t, err)

	recordLengthAligned := alignedRecordLength(length)
	recordOffset := int(tail)
	toEndOfBuffer := testCapacity - recordOffset

	require.Equal(t, int32(broadcast.PaddingMsgTypeID), buf.GetInt32(record.TypeOffset(recordOffset)))
	require.Equal(t, int32(toEndOfBuffer), buf.GetInt32(record.LengthOffset(recordOffset)))

	newTail := tail + int64(toEndOfBuffer)
	require.Equal(t, int32(testMsgTypeID), buf.GetInt32(record.TypeOffset(0)))
	require.Equal(t, newTail, buf.GetInt64Volatile(broadcast.LatestIndex(testCapacity)))
	wantTail := newTail + int64(recordLengthAligned)
	require.Equal(t, wantTail, buf.GetInt64Volatile(broadcast.TailIndex(testCapacity)))
	expectedTailIntent := tail + int64(recordLengthAligned+toEndOfBuffer)
	tailIntentIdx := broadcast.TailIntentIndex(testCapacity)
	require.Equal(t, expectedTailIntent, buf.GetInt64Volatile(tailIntentIdx))
}

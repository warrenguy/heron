package broadcast_test

import (
	"bytes"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/record"
)

func TestBroadcastTransmitterCapacity(t *testing.T) {
	tx := newTransmitter(t, newTestBuffer(t))
	if tx.Capacity() != testCapacity {
		t.Fatalf("capacity=%d want %d", tx.Capacity(), testCapacity)
	}
}

func TestBroadcastTransmitterInvalidCapacity(t *testing.T) {
	_, err := broadcast.NewBroadcastTransmitter(invalidCapacityBuffer())
	if err == nil {
		t.Fatal("expected error for non-power-of-two capacity")
	}
	_, err = broadcast.NewBroadcastTransmitter(
		atomic.Wrap(make([]byte, broadcast.TotalBufferLength(8))),
	)
	if err == nil {
		t.Fatal("expected error for data capacity below minimum")
	}
}

func TestBroadcastTransmitterMaxMessageLengthExceeded(t *testing.T) {
	tx := newTransmitter(t, newTestBuffer(t))
	src := make([]byte, tx.MaxMsgLength()+1)
	if err := tx.Transmit(testMsgTypeID, src); err == nil {
		t.Fatal("expected error when max message length exceeded")
	}
}

func TestBroadcastTransmitterInvalidMessageTypeID(t *testing.T) {
	tx := newTransmitter(t, newTestBuffer(t))
	if err := tx.Transmit(-1, make([]byte, 32)); err == nil {
		t.Fatal("expected error for invalid message type id")
	}
}

func TestBroadcastTransmitterIntoEmptyBuffer(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := testPayload8
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	recordLength := len(src) + broadcast.HeaderLength
	recordLengthAligned := alignedRecordLength(len(src))

	tailIntentIdx := broadcast.TailIntentIndex(testCapacity)
	tailIdx := broadcast.TailIndex(testCapacity)
	latestIdx := broadcast.LatestIndex(testCapacity)
	if got := buf.GetInt64Volatile(tailIntentIdx); got != int64(recordLengthAligned) {
		t.Fatalf("tailIntent=%d want %d", got, recordLengthAligned)
	}
	if got := buf.GetInt64Volatile(latestIdx); got != 0 {
		t.Fatalf("latest=%d want 0", got)
	}
	if got := buf.GetInt64Volatile(tailIdx); got != int64(recordLengthAligned) {
		t.Fatalf("tail=%d want %d", got, recordLengthAligned)
	}
	if got := buf.GetInt32(record.LengthOffset(0)); got != int32(recordLength) {
		t.Fatalf("record length=%d want %d", got, recordLength)
	}
	if got := buf.GetInt32(record.TypeOffset(0)); got != testMsgTypeID {
		t.Fatalf("typeId=%d want %d", got, testMsgTypeID)
	}
	got := buf.Bytes()[broadcast.MsgOffset(0) : broadcast.MsgOffset(0)+len(src)]
	if !bytes.Equal(got, src) {
		t.Fatalf("payload=%v want %v", got, src)
	}
}

func TestBroadcastTransmitterIntoUsedBuffer(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	const tail = broadcast.Alignment * 3
	buf.PutInt64(broadcast.TailIndex(testCapacity), tail)

	src := []byte{9, 8, 7, 6, 5, 4, 3, 2}
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	recordLengthAligned := alignedRecordLength(len(src))
	recordOffset := int(tail)

	tailIntentIdx := broadcast.TailIntentIndex(testCapacity)
	tailIdx := broadcast.TailIndex(testCapacity)
	latestIdx := broadcast.LatestIndex(testCapacity)
	wantTail := tail + int64(recordLengthAligned)
	if got := buf.GetInt64Volatile(tailIntentIdx); got != wantTail {
		t.Fatalf("tailIntent=%d want %d", got, wantTail)
	}
	if got := buf.GetInt64Volatile(latestIdx); got != tail {
		t.Fatalf("latest=%d want %d", got, tail)
	}
	if got := buf.GetInt64Volatile(tailIdx); got != wantTail {
		t.Fatalf("tail=%d want %d", got, wantTail)
	}
	if got := buf.GetInt32(record.TypeOffset(recordOffset)); got != testMsgTypeID {
		t.Fatalf("typeId=%d want %d", got, testMsgTypeID)
	}
}

func TestBroadcastTransmitterIntoEndOfBuffer(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := []byte{1, 1, 2, 2, 3, 3, 4, 4}
	recordLengthAligned := alignedRecordLength(len(src))
	tail := int64(testCapacity - recordLengthAligned)

	buf.PutInt64(broadcast.TailIndex(testCapacity), tail)
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	recordOffset := int(tail)
	wantTail := tail + int64(recordLengthAligned)
	if got := buf.GetInt64Volatile(broadcast.TailIndex(testCapacity)); got != wantTail {
		t.Fatalf("tail=%d want %d", got, wantTail)
	}
	if got := buf.GetInt32(record.TypeOffset(recordOffset)); got != testMsgTypeID {
		t.Fatalf("typeId=%d want %d", got, testMsgTypeID)
	}
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
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	recordLengthAligned := alignedRecordLength(length)
	recordOffset := int(tail)
	toEndOfBuffer := testCapacity - recordOffset

	if got := buf.GetInt32(record.TypeOffset(recordOffset)); got != broadcast.PaddingMsgTypeID {
		t.Fatalf("padding typeId=%d want %d", got, broadcast.PaddingMsgTypeID)
	}
	if got := buf.GetInt32(record.LengthOffset(recordOffset)); got != int32(toEndOfBuffer) {
		t.Fatalf("padding length=%d want %d", got, toEndOfBuffer)
	}

	newTail := tail + int64(toEndOfBuffer)
	if got := buf.GetInt32(record.TypeOffset(0)); got != testMsgTypeID {
		t.Fatalf("wrapped typeId=%d want %d", got, testMsgTypeID)
	}
	if got := buf.GetInt64Volatile(broadcast.LatestIndex(testCapacity)); got != newTail {
		t.Fatalf("latest=%d want %d", got, newTail)
	}
	wantTail := newTail + int64(recordLengthAligned)
	if got := buf.GetInt64Volatile(broadcast.TailIndex(testCapacity)); got != wantTail {
		t.Fatalf("tail=%d want %d", got, wantTail)
	}
	expectedTailIntent := tail + int64(recordLengthAligned+toEndOfBuffer)
	tailIntentIdx := broadcast.TailIntentIndex(testCapacity)
	if got := buf.GetInt64Volatile(tailIntentIdx); got != expectedTailIntent {
		t.Fatalf("tailIntent=%d want %d", got, expectedTailIntent)
	}
}

package broadcast_test

import (
	"bytes"
	"testing"

	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/record"
)

func TestBroadcastReceiverCapacity(t *testing.T) {
	rx := newReceiver(t, newTestBuffer(t))
	if rx.Capacity() != testCapacity {
		t.Fatalf("capacity=%d want %d", rx.Capacity(), testCapacity)
	}
}

func TestBroadcastReceiverInvalidCapacity(t *testing.T) {
	_, err := broadcast.NewBroadcastReceiver(invalidCapacityBuffer())
	if err == nil {
		t.Fatal("expected error for non-power-of-two capacity")
	}
}

func TestBroadcastReceiverNotLappedBeforeReception(t *testing.T) {
	rx := newReceiver(t, newTestBuffer(t))
	if rx.LappedCount() != 0 {
		t.Fatalf("lappedCount=%d want 0", rx.LappedCount())
	}
}

func TestBroadcastReceiverEmptyBuffer(t *testing.T) {
	rx := newReceiver(t, newTestBuffer(t))
	if rx.ReceiveNext() {
		t.Fatal("expected no message from empty buffer")
	}
}

func TestBroadcastReceiverFirstMessage(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	const length = 8
	src := []byte{10, 11, 12, 13, 14, 15, 16, 17}
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	rx := newReceiver(t, buf)
	if !rx.ReceiveNext() {
		t.Fatal("expected first message")
	}
	if rx.TypeID() != testMsgTypeID {
		t.Fatalf("typeId=%d want %d", rx.TypeID(), testMsgTypeID)
	}
	if rx.Offset() != broadcast.MsgOffset(0) {
		t.Fatalf("offset=%d want %d", rx.Offset(), broadcast.MsgOffset(0))
	}
	if rx.Length() != length {
		t.Fatalf("length=%d want %d", rx.Length(), length)
	}
	if !rx.Validate() {
		t.Fatal("expected record to validate")
	}
	got := copyPayload(rx)
	if !bytes.Equal(got, src) {
		t.Fatalf("payload=%v want %v", got, src)
	}
}

func TestBroadcastReceiverTwoMessages(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)
	rx := newReceiver(t, buf)

	src := testPayload8
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	if !rx.ReceiveNext() {
		t.Fatal("expected first message")
	}
	if rx.Offset() != broadcast.MsgOffset(0) {
		t.Fatalf("first offset=%d want %d", rx.Offset(), broadcast.MsgOffset(0))
	}
	if !rx.Validate() {
		t.Fatal("expected first record to validate")
	}

	if !rx.ReceiveNext() {
		t.Fatal("expected second message")
	}
	recordOffsetTwo := alignedRecordLength(len(src))
	if rx.Offset() != broadcast.MsgOffset(recordOffsetTwo) {
		t.Fatalf("second offset=%d want %d", rx.Offset(), broadcast.MsgOffset(recordOffsetTwo))
	}
	if !rx.Validate() {
		t.Fatal("expected second record to validate")
	}
}

func TestBroadcastReceiverLateJoinTransmission(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := testPayload8
	for range 3*testCapacity/16 + 1 {
		if err := tx.Transmit(testMsgTypeID, src); err != nil {
			t.Fatal(err)
		}
	}

	actualLatest := buf.GetInt64Volatile(broadcast.LatestIndex(testCapacity))
	buf.PutInt64Release(broadcast.LatestIndex(testCapacity), 0)

	rx, err := broadcast.NewBroadcastReceiver(buf)
	if err != nil {
		t.Fatal(err)
	}
	buf.PutInt64Release(broadcast.LatestIndex(testCapacity), actualLatest)

	if !rx.ReceiveNext() {
		t.Fatal("expected message after late join catch-up")
	}
	if rx.LappedCount() == 0 {
		t.Fatal("expected lapped count > 0 on late join")
	}
	if !rx.Validate() {
		t.Fatal("expected record to validate after catch-up")
	}
}

func TestBroadcastReceiverPaddingAndWrap(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)
	rx := newReceiver(t, buf)

	small := testPayload8
	beforeWrap := messagesBeforeWrap(testCapacity, len(small))
	for range beforeWrap {
		if err := tx.Transmit(testMsgTypeID, small); err != nil {
			t.Fatal(err)
		}
		if !rx.ReceiveNext() {
			t.Fatal("expected small message before wrap")
		}
	}

	const length = 120
	src := make([]byte, length)
	for i := range src {
		src[i] = byte(i)
	}
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	if !rx.ReceiveNext() {
		t.Fatal("expected message after padding")
	}
	if rx.TypeID() != testMsgTypeID {
		t.Fatalf("typeId=%d want %d", rx.TypeID(), testMsgTypeID)
	}
	if rx.Length() != length {
		t.Fatalf("length=%d want %d", rx.Length(), length)
	}
	if !rx.Validate() {
		t.Fatal("expected record to validate")
	}
}

func TestBroadcastReceiverLengthDefensive(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := testPayload8
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	rx := newReceiver(t, buf)
	if !rx.ReceiveNext() {
		t.Fatal("expected first message")
	}
	if rx.Length() != len(src) {
		t.Fatalf("length=%d want %d", rx.Length(), len(src))
	}

	buf.PutInt32(record.LengthOffset(0), broadcast.HeaderLength-1)
	if rx.Length() != 0 {
		t.Fatalf("length=%d want 0 for corrupt record length", rx.Length())
	}
}

func TestBroadcastReceiverRecordInvalidatedByOverwrite(t *testing.T) {
	buf := newTestBuffer(t)
	tx := newTransmitter(t, buf)

	src := testPayload8
	if err := tx.Transmit(testMsgTypeID, src); err != nil {
		t.Fatal(err)
	}

	rx := newReceiver(t, buf)
	if !rx.ReceiveNext() {
		t.Fatal("expected first message")
	}
	if rx.TypeID() != testMsgTypeID {
		t.Fatalf("typeId=%d want %d", rx.TypeID(), testMsgTypeID)
	}

	recordLengthAligned := alignedRecordLength(len(src))
	// Advance tailIntent past cursor+capacity without receiver catching up.
	buf.PutInt64Release(
		broadcast.TailIntentIndex(testCapacity),
		int64(recordLengthAligned)+int64(testCapacity-recordLengthAligned),
	)

	if rx.Validate() {
		t.Fatal("expected record to be invalid after overwrite")
	}
}

package ringbuffer_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	testMsgTypeID    = 7
	testDataCapacity = 4096
)

func newRingBufferBuffer(dataCapacity int) (*ringbuffer.ManyToOneRingBuffer, aeronatomic.AtomicBuffer, error) {
	buf := make([]byte, ringbuffer.TotalBufferLength(dataCapacity))
	atomicBuffer := aeronatomic.Wrap(buf)
	ring, err := ringbuffer.NewManyToOneRingBuffer(atomicBuffer)
	if err != nil {
		return nil, aeronatomic.AtomicBuffer{}, err
	}
	return ring, atomicBuffer, nil
}

func seedTrailerPositions(
	buffer aeronatomic.AtomicBuffer,
	dataCapacity int,
	head, tail int64,
) {
	buffer.PutInt64Release(ringbuffer.HeadIndex(dataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(dataCapacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(dataCapacity), head)
}

func alignedRecordLength(payloadLen int) int {
	return record.AlignRecordLength(payloadLen + ringbuffer.HeaderLength)
}

func clampMaxMsgLen(maxLen, floor int) int {
	if maxLen < floor {
		return floor
	}
	return maxLen
}

func seedCommitAtEndCase(
	buffer aeronatomic.AtomicBuffer,
	dataCapacity, msgLen int,
) {
	recordAligned := alignedRecordLength(msgLen)
	head := int64(dataCapacity - recordAligned)
	tail := head + int64(recordAligned)
	seedTrailerPositions(buffer, dataCapacity, head, tail)
	buffer.PutInt32Release(record.LengthOffset(int(head)), int32(recordAligned))
	buffer.PutInt32(record.TypeOffset(int(head)), int32(testMsgTypeID))
}

func comparePayloadSlices(tb ringBufferTestReporter, got, want [][]byte) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("read %d messages want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			tb.Fatalf("message %d: payload mismatch", i)
		}
	}
}

func assertConsumerCaughtUp(tb ringBufferTestReporter, ring *ringbuffer.ManyToOneRingBuffer) {
	tb.Helper()
	if ring.ConsumerPosition() != ring.ProducerPosition() {
		tb.Fatalf("consumer=%d producer=%d", ring.ConsumerPosition(), ring.ProducerPosition())
	}
}

func drainReadMessages(ring *ringbuffer.ManyToOneRingBuffer) [][]byte {
	var got [][]byte
	for {
		n := ring.Read(func(_ int, buffer aeronatomic.AtomicBuffer, index, length int) {
			got = append(got, readPayload(buffer, index, length))
		})
		if n == 0 {
			break
		}
	}
	return got
}

func readPayload(buffer aeronatomic.AtomicBuffer, index, length int) []byte {
	b := buffer.Bytes()
	out := make([]byte, length)
	copy(out, b[index:index+length])
	return out
}

func assertConsumerUnblockedPadding(
	tb ringBufferTestReporter,
	buffer aeronatomic.AtomicBuffer,
	consumerIndex, wantPaddingLength int,
) {
	tb.Helper()
	if buffer.GetInt32(record.TypeOffset(consumerIndex)) != ringbuffer.PaddingMsgTypeID {
		tb.Fatalf("type at %d: want padding", consumerIndex)
	}
	if got := buffer.GetInt32(record.LengthOffset(consumerIndex)); got != int32(wantPaddingLength) {
		tb.Fatalf("padding length at %d: got %d want %d", consumerIndex, got, wantPaddingLength)
	}
}

type unblockCase struct {
	name          string
	head, tail    int64
	lengthIndex   int
	lengthValue   int32
	wantUnblock   bool
	paddingIndex  int
	paddingLength int
}

const unblockMessageLength = ringbuffer.Alignment * 4

func unblockGapWithZerosCase(messageLength int) unblockCase {
	head := int64(messageLength)
	return unblockCase{
		name:          "GapWithZeros",
		head:          head,
		tail:          head * 3,
		lengthIndex:   messageLength * 2,
		lengthValue:   int32(messageLength),
		wantUnblock:   true,
		paddingIndex:  messageLength,
		paddingLength: messageLength,
	}
}

func seedUnblockCase(
	buffer aeronatomic.AtomicBuffer,
	dataCapacity int,
	tc unblockCase,
) {
	seedTrailerPositions(buffer, dataCapacity, tc.head, tc.tail)
	if tc.lengthIndex != 0 || tc.lengthValue != 0 {
		buffer.PutInt32(tc.lengthIndex, tc.lengthValue)
	}
}

func assertUnblockResult(
	tb ringBufferTestReporter,
	ring *ringbuffer.ManyToOneRingBuffer,
	buffer aeronatomic.AtomicBuffer,
	tc unblockCase,
) {
	tb.Helper()
	if got := ring.Unblock(); got != tc.wantUnblock {
		tb.Fatalf("Unblock()=%v want %v", got, tc.wantUnblock)
	}
	if tc.paddingLength > 0 {
		assertConsumerUnblockedPadding(tb, buffer, tc.paddingIndex, tc.paddingLength)
	}
}

type ringBufferTestReporter interface {
	Helper()
	Fatalf(string, ...any)
}

func newRingBufferForTest(
	tb ringBufferTestReporter,
	dataCapacity int,
) (*ringbuffer.ManyToOneRingBuffer, aeronatomic.AtomicBuffer) {
	tb.Helper()
	ring, atomicBuffer, err := newRingBufferBuffer(dataCapacity)
	if err != nil {
		tb.Fatalf("NewManyToOneRingBuffer: %v", err)
	}
	return ring, atomicBuffer
}

func newTestRingBuffer(tb testing.TB, dataCapacity int) (*ringbuffer.ManyToOneRingBuffer, aeronatomic.AtomicBuffer) {
	return newRingBufferForTest(tb, dataCapacity)
}

func TestNewManyToOneRingBuffer_Capacity(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	if ring.Capacity() != testDataCapacity {
		t.Fatalf("Capacity()=%d want %d", ring.Capacity(), testDataCapacity)
	}
}

func TestNewManyToOneRingBuffer_InvalidCapacity(t *testing.T) {
	cases := []struct {
		name         string
		dataCapacity int
		bufferLen    int // 0 → TotalBufferLength(dataCapacity)
	}{
		{"below min", ringbuffer.MinCapacity - 1, 0},
		{"not power of two", 777, 777 + ringbuffer.TrailerLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			total := tc.bufferLen
			if total == 0 {
				total = ringbuffer.TotalBufferLength(tc.dataCapacity)
			}
			_, err := ringbuffer.NewManyToOneRingBuffer(aeronatomic.Wrap(make([]byte, total)))
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestManyToOneRingBuffer_MaxMsgLength(t *testing.T) {
	cases := []struct {
		dataCapacity int
		want         int
	}{
		{ringbuffer.MinCapacity, 0},
		{16, 8},
		{1024, 128},
		{testDataCapacity, 512},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("capacity%d", tc.dataCapacity), func(t *testing.T) {
			ring, _ := newTestRingBuffer(t, tc.dataCapacity)
			if got := ring.MaxMsgLength(); got != tc.want {
				t.Fatalf("MaxMsgLength()=%d want %d", got, tc.want)
			}
		})
	}
}

func TestManyToOneRingBuffer_WriteRead(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	payload := []byte("abcdefgh")
	src := aeronatomic.Wrap(payload)

	ok, err := ring.Write(testMsgTypeID, src, 0, len(payload))
	if err != nil || !ok {
		t.Fatalf("Write ok=%v err=%v", ok, err)
	}

	var gotType int
	var gotPayload []byte
	n := ring.Read(func(msgTypeID int, buffer aeronatomic.AtomicBuffer, index, length int) {
		gotType = msgTypeID
		gotPayload = readPayload(buffer, index, length)
	})
	if n != 1 {
		t.Fatalf("Read messages=%d want 1", n)
	}
	if gotType != testMsgTypeID {
		t.Fatalf("type=%d want %d", gotType, testMsgTypeID)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload=%q want %q", gotPayload, payload)
	}
	if ring.ConsumerPosition() != ring.ProducerPosition() {
		t.Fatalf("consumer=%d producer=%d", ring.ConsumerPosition(), ring.ProducerPosition())
	}
}

func TestManyToOneRingBuffer_RejectWriteInsufficientSpace(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 200
	head := int64(0)
	tail := head + int64(testDataCapacity-record.AlignRecordLength(length-ringbuffer.Alignment))

	seedTrailerPositions(buffer, testDataCapacity, head, tail)

	src := aeronatomic.Wrap(make([]byte, length))
	ok, err := ring.Write(testMsgTypeID, src, 0, length)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected write rejection")
	}
}

func TestManyToOneRingBuffer_RejectWriteWhenFull(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 8
	head := int64(0)
	tail := head + int64(testDataCapacity)

	seedTrailerPositions(buffer, testDataCapacity, head, tail)

	src := aeronatomic.Wrap(make([]byte, length))
	ok, err := ring.Write(testMsgTypeID, src, 0, length)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected write rejection when full")
	}
}

func TestManyToOneRingBuffer_WriteWrapPadding(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 200
	recordAligned := alignedRecordLength(length)
	tail := int64(testDataCapacity - ringbuffer.HeaderLength)
	head := tail

	seedTrailerPositions(buffer, testDataCapacity, head, tail)

	payload := bytes.Repeat([]byte{0xAB}, length)
	src := aeronatomic.Wrap(payload)
	ok, err := ring.Write(testMsgTypeID, src, 0, length)
	if err != nil || !ok {
		t.Fatalf("Write ok=%v err=%v", ok, err)
	}

	paddingIndex := int(tail)
	if buffer.GetInt32(record.LengthOffset(paddingIndex)) != ringbuffer.HeaderLength {
		t.Fatalf("padding length at %d", paddingIndex)
	}
	if buffer.GetInt32(record.TypeOffset(paddingIndex)) != ringbuffer.PaddingMsgTypeID {
		t.Fatal("expected padding type at wrap")
	}

	var got []byte
	ring.Read(func(_ int, buf aeronatomic.AtomicBuffer, index, msgLen int) {})
	ring.Read(func(_ int, buf aeronatomic.AtomicBuffer, index, msgLen int) {
		got = readPayload(buf, index, msgLen)
	})
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch after wrap")
	}
	if ring.ProducerPosition() != tail+int64(recordAligned)+int64(ringbuffer.HeaderLength) {
		t.Fatalf("producer=%d", ring.ProducerPosition())
	}
}

func TestManyToOneRingBuffer_WriteWrapHeadEqualsTail(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 200
	tail := int64(testDataCapacity - ringbuffer.HeaderLength)
	head := tail

	seedTrailerPositions(buffer, testDataCapacity, head, tail)

	payload := bytes.Repeat([]byte{0xCD}, length)
	ok, err := ring.Write(testMsgTypeID, aeronatomic.Wrap(payload), 0, length)
	if err != nil || !ok {
		t.Fatalf("Write ok=%v err=%v", ok, err)
	}
	if buffer.GetInt32(record.TypeOffset(int(tail))) != ringbuffer.PaddingMsgTypeID {
		t.Fatal("expected padding record at buffer end")
	}
}

func TestManyToOneRingBuffer_ReadEmpty(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	n := ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
		t.Fatal("handler should not run")
	})
	if n != 0 {
		t.Fatalf("messages=%d want 0", n)
	}
}

func TestManyToOneRingBuffer_ReadSkipsInFlight(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	recordLength := ringbuffer.HeaderLength + 16
	buffer.PutInt32Release(record.LengthOffset(0), int32(-recordLength))

	n := ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
		t.Fatal("handler should not run")
	})
	if n != 0 {
		t.Fatalf("messages=%d want 0", n)
	}
	if ring.ConsumerPosition() != 0 {
		t.Fatalf("consumer=%d want 0", ring.ConsumerPosition())
	}
}

func TestManyToOneRingBuffer_ReadTwoMessages(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	msgLen := 16
	payload := bytes.Repeat([]byte{1}, msgLen)

	for range 2 {
		ok, err := ring.Write(testMsgTypeID, aeronatomic.Wrap(payload), 0, msgLen)
		if err != nil || !ok {
			t.Fatalf("Write ok=%v err=%v", ok, err)
		}
	}

	count := 0
	n := ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
		count++
	})
	if n != 2 || count != 2 {
		t.Fatalf("messages=%d handler=%d", n, count)
	}
}

func TestManyToOneRingBuffer_ReadWithLimit_ZeroAndNegative(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	payload := bytes.Repeat([]byte{2}, 16)

	if _, err := ring.Write(testMsgTypeID, aeronatomic.Wrap(payload), 0, len(payload)); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		limit int
	}{
		{"zero", 0},
		{"negative", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			n := ring.ReadWithLimit(func(int, aeronatomic.AtomicBuffer, int, int) {
				called = true
			}, tc.limit)
			if n != 0 {
				t.Fatalf("messages=%d want 0", n)
			}
			if called {
				t.Fatal("handler should not run")
			}
			if ring.ConsumerPosition() != 0 {
				t.Fatalf("consumer=%d want 0", ring.ConsumerPosition())
			}
		})
	}
}

func TestManyToOneRingBuffer_ReadWithLimit(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	msgLen := 16
	payload := bytes.Repeat([]byte{2}, msgLen)

	for range 2 {
		_, err := ring.Write(testMsgTypeID, aeronatomic.Wrap(payload), 0, msgLen)
		if err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	n := ring.ReadWithLimit(func(int, aeronatomic.AtomicBuffer, int, int) {
		count++
	}, 1)
	if n != 1 || count != 1 {
		t.Fatalf("messages=%d handler=%d", n, count)
	}
	if ring.ConsumerPosition() == ring.ProducerPosition() {
		t.Fatal("expected unread message remaining")
	}
}

func TestManyToOneRingBuffer_TryClaimCommitRead(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 10
	payload := bytes.Repeat([]byte{3}, length)

	index, err := ring.TryClaim(testMsgTypeID, length)
	if err != nil {
		t.Fatal(err)
	}
	if index != ringbuffer.EncodedMsgOffset(0) {
		t.Fatalf("index=%d want %d", index, ringbuffer.EncodedMsgOffset(0))
	}

	buffer.PutBytes(index, payload)
	if err := ring.Commit(index); err != nil {
		t.Fatal(err)
	}

	var got []byte
	ring.Read(func(_ int, buf aeronatomic.AtomicBuffer, off, msgLen int) {
		got = readPayload(buf, off, msgLen)
	})
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload=%q want %q", got, payload)
	}
}

func TestManyToOneRingBuffer_TryClaimAbortSkippedOnRead(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	length := 10

	index, err := ring.TryClaim(testMsgTypeID, length)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.Abort(index); err != nil {
		t.Fatal(err)
	}

	called := false
	n := ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
		called = true
	})
	if n != 0 || called {
		t.Fatal("aborted claim should not deliver a message")
	}
}

func TestManyToOneRingBuffer_ClaimCapacityStaleHeadCache(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	const headCache = int64(0)
	head := int64(testDataCapacity * 3)
	tail := head

	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(testDataCapacity), headCache)
	buffer.PutInt64Release(ringbuffer.HeadIndex(testDataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(testDataCapacity), tail)

	wantIndex := ringbuffer.EncodedMsgOffset(int(tail) & (testDataCapacity - 1))
	index, err := ring.TryClaim(testMsgTypeID, 8)
	if err != nil {
		t.Fatal(err)
	}
	if index == ringbuffer.InsufficientCapacity {
		t.Fatal("TryClaim returned InsufficientCapacity with stale head cache but consumer caught up")
	}
	if index != wantIndex {
		t.Fatalf("index=%d want %d", index, wantIndex)
	}
	if err := ring.Abort(index); err != nil {
		t.Fatal(err)
	}

	src := aeronatomic.Wrap([]byte("abcdefgh"))
	ok, err := ring.Write(testMsgTypeID, src, 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("Write returned false with stale head cache but consumer caught up")
	}
}

func TestManyToOneRingBuffer_TryClaimInsufficientCapacity(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	head := int64(0)
	tail := int64(testDataCapacity)

	seedTrailerPositions(buffer, testDataCapacity, head, tail)

	index, err := ring.TryClaim(testMsgTypeID, 8)
	if err != nil {
		t.Fatal(err)
	}
	if index != ringbuffer.InsufficientCapacity {
		t.Fatalf("index=%d want %d", index, ringbuffer.InsufficientCapacity)
	}
}

func TestManyToOneRingBuffer_TryClaimWrapPadding(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	length := 10
	recordAligned := alignedRecordLength(length)
	head := int64(248)
	padding := 22
	tail := int64(testDataCapacity - padding)

	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(testDataCapacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(testDataCapacity), tail)

	index, err := ring.TryClaim(testMsgTypeID, length)
	if err != nil {
		t.Fatal(err)
	}
	if index != ringbuffer.EncodedMsgOffset(0) {
		t.Fatalf("index=%d want encoded offset at 0", index)
	}

	paddingIndex := int(tail)
	if buffer.GetInt32(record.TypeOffset(paddingIndex)) != ringbuffer.PaddingMsgTypeID {
		t.Fatal("expected padding at end of buffer")
	}
	if ring.ProducerPosition() != tail+int64(recordAligned)+int64(padding) {
		t.Fatalf("producer=%d", ring.ProducerPosition())
	}
}

func TestManyToOneRingBuffer_InvalidTypeAndLength(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	src := aeronatomic.Wrap(make([]byte, 8))

	if _, err := ring.Write(0, src, 0, 8); err == nil {
		t.Fatal("expected error for type 0")
	}
	index, err := ring.TryClaim(-1, 8)
	if err == nil {
		t.Fatal("expected error for invalid type")
	}
	if index != -1 {
		t.Fatalf("TryClaim index=%d want -1", index)
	}
	if _, err := ring.Write(testMsgTypeID, src, 0, -1); err == nil {
		t.Fatal("expected error for negative length")
	}
	if _, err := ring.Write(testMsgTypeID, src, 0, ring.MaxMsgLength()+1); err == nil {
		t.Fatal("expected error for length over max")
	}
}

func TestManyToOneRingBuffer_CommitZeroLengthNotInFlight(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	index, err := ring.TryClaim(testMsgTypeID, 8)
	if err != nil {
		t.Fatal(err)
	}
	recordIndex := index - ringbuffer.HeaderLength
	buffer.PutInt32(record.LengthOffset(recordIndex), 0)

	if err := ring.Commit(index); err == nil {
		t.Fatal("expected error on commit with zero length field")
	} else if err.Error() != "ringbuffer: claimed space not in flight" {
		t.Fatalf("Commit err=%v want claimed space not in flight", err)
	}
}

func TestManyToOneRingBuffer_CommitAbortStateErrors(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	index, err := ring.TryClaim(testMsgTypeID, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.Commit(index); err != nil {
		t.Fatal(err)
	}
	if err := ring.Commit(index); err == nil {
		t.Fatal("expected error on double commit")
	}

	index2, err := ring.TryClaim(testMsgTypeID, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.Abort(index2); err != nil {
		t.Fatal(err)
	}
	if err := ring.Abort(index2); err == nil {
		t.Fatal("expected error on double abort")
	}
}

func TestManyToOneRingBuffer_Size(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	if ring.Size() != 0 {
		t.Fatalf("empty size=%d", ring.Size())
	}

	msgLen := 8
	ok, err := ring.Write(testMsgTypeID, aeronatomic.Wrap(make([]byte, msgLen)), 0, msgLen)
	if err != nil || !ok {
		t.Fatalf("Write ok=%v err=%v", ok, err)
	}

	want := alignedRecordLength(msgLen)
	if ring.Size() != want {
		t.Fatalf("size=%d want %d", ring.Size(), want)
	}
}

func writeHelloWorld(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) []byte {
	t.Helper()
	msg := []byte("Hello World")
	ok, err := ring.Write(testMsgTypeID, aeronatomic.Wrap(msg), 0, len(msg))
	if err != nil || !ok {
		t.Fatalf("Write ok=%v err=%v", ok, err)
	}
	return msg
}

func TestManyToOneRingBuffer_ControlledRead(t *testing.T) {
	type controlledCase struct {
		name         string
		writeCount   int
		messageLimit int
		action       func(invocation int, ring *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction
		wantMessages int
		wantHandler  int
		checkPayload bool
		setup        func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer, buffer aeronatomic.AtomicBuffer)
		assertAfter  func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer)
	}

	cases := []controlledCase{
		{
			name:         "Continue",
			writeCount:   1,
			messageLimit: 1,
			action: func(_ int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				return ringbuffer.ControlledContinue
			},
			wantMessages: 1,
			wantHandler:  1,
			checkPayload: true,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				if ring.ConsumerPosition() != ring.ProducerPosition() {
					t.Fatal("expected consumer caught up")
				}
			},
		},
		{
			name:         "Abort",
			writeCount:   1,
			messageLimit: 1,
			action: func(_ int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				return ringbuffer.ControlledAbort
			},
			wantMessages: 0,
			wantHandler:  1,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				if ring.ConsumerPosition() != 0 {
					t.Fatalf("consumer=%d want 0", ring.ConsumerPosition())
				}
			},
		},
		{
			name:       "AbortSecondMessage",
			writeCount: 2,
			action: func(inv int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				if inv == 1 {
					return ringbuffer.ControlledContinue
				}
				return ringbuffer.ControlledAbort
			},
			wantMessages: 1,
			wantHandler:  2,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				if ring.ConsumerPosition() == ring.ProducerPosition() {
					t.Fatal("expected partial consumption")
				}
			},
		},
		{
			name:       "CommitAtBufferEndWrapsHeadIndex",
			writeCount: 1,
			action: func(_ int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				return ringbuffer.ControlledCommit
			},
			wantMessages: 1,
			wantHandler:  1,
			setup: func(t *testing.T, _ *ringbuffer.ManyToOneRingBuffer, buffer aeronatomic.AtomicBuffer) {
				t.Helper()
				seedCommitAtEndCase(buffer, 64, 8)
			},
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				t.Helper()
				if ring.ConsumerPosition() != ring.ProducerPosition() {
					t.Fatalf("consumer=%d producer=%d", ring.ConsumerPosition(), ring.ProducerPosition())
				}
				n := ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
					t.Fatal("unexpected message after commit at buffer end")
				})
				if n != 0 {
					t.Fatalf("second read messages=%d want 0", n)
				}
			},
		},
		{
			name:       "CommitEachMessage",
			writeCount: 2,
			action: func(inv int, ring *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				if inv == 1 {
					if ring.ConsumerPosition() != 0 {
						t.Fatalf("first commit consumer=%d want 0", ring.ConsumerPosition())
					}
				} else if ring.ConsumerPosition() != ring.ProducerPosition()/2 {
					t.Fatalf("second commit consumer=%d producer/2=%d",
						ring.ConsumerPosition(), ring.ProducerPosition()/2)
				}
				return ringbuffer.ControlledCommit
			},
			wantMessages: 2,
			wantHandler:  2,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				if ring.ConsumerPosition() != ring.ProducerPosition() {
					t.Fatal("expected full consumption")
				}
			},
		},
		{
			name:       "Break",
			writeCount: 2,
			action: func(_ int, _ *ringbuffer.ManyToOneRingBuffer) ringbuffer.ControlledAction {
				return ringbuffer.ControlledBreak
			},
			wantMessages: 1,
			wantHandler:  1,
			assertAfter: func(t *testing.T, ring *ringbuffer.ManyToOneRingBuffer) {
				if ring.ConsumerPosition() == ring.ProducerPosition() {
					t.Fatal("expected unread message after break")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataCapacity := testDataCapacity
			if tc.setup != nil {
				dataCapacity = 64
			}
			ring, buffer := newTestRingBuffer(t, dataCapacity)
			if tc.setup != nil {
				tc.setup(t, ring, buffer)
			}
			var msg []byte
			for range tc.writeCount {
				if tc.setup != nil {
					msg = []byte("Hello World")
					continue
				}
				msg = writeHelloWorld(t, ring)
			}

			var (
				gotType, gotIndex, gotLen int
				gotPayload                []byte
				invocations               int
			)
			handler := func(msgTypeID int, buffer aeronatomic.AtomicBuffer, index, length int) ringbuffer.ControlledAction {
				invocations++
				action := tc.action(invocations, ring)
				if tc.checkPayload && invocations == 1 {
					gotType = msgTypeID
					gotIndex = index
					gotLen = length
					gotPayload = readPayload(buffer, index, length)
				}
				return action
			}

			var messages int
			if tc.messageLimit > 0 {
				messages = ring.ControlledReadWithLimit(handler, tc.messageLimit)
			} else {
				messages = ring.ControlledRead(handler)
			}

			if messages != tc.wantMessages {
				t.Fatalf("messages=%d want %d", messages, tc.wantMessages)
			}
			if invocations != tc.wantHandler {
				t.Fatalf("handler invocations=%d want %d", invocations, tc.wantHandler)
			}
			if tc.checkPayload {
				if gotType != testMsgTypeID || gotIndex != ringbuffer.EncodedMsgOffset(0) || gotLen != len(msg) {
					t.Fatalf("type=%d index=%d len=%d", gotType, gotIndex, gotLen)
				}
				if !bytes.Equal(gotPayload, msg) {
					t.Fatalf("payload=%q want %q", gotPayload, msg)
				}
			}
			if tc.assertAfter != nil {
				tc.assertAfter(t, ring)
			}
		})
	}
}

func TestManyToOneRingBuffer_Unblock(t *testing.T) {
	messageLength := unblockMessageLength
	head := int64(messageLength)
	emptyPos := int64(messageLength)

	cases := []unblockCase{
		{
			name:        "Empty",
			head:        emptyPos,
			tail:        emptyPos,
			wantUnblock: false,
		},
		{
			name:          "StuckNegativeLength",
			head:          head,
			tail:          head * 2,
			lengthIndex:   messageLength,
			lengthValue:   int32(-messageLength),
			wantUnblock:   true,
			paddingIndex:  messageLength,
			paddingLength: messageLength,
		},
		{
			name:          "WhenFullWithHeader",
			head:          head,
			tail:          head + int64(testDataCapacity),
			lengthIndex:   messageLength,
			lengthValue:   int32(-messageLength),
			wantUnblock:   true,
			paddingIndex:  messageLength,
			paddingLength: messageLength,
		},
		{
			name:          "WhenFullWithoutHeader",
			head:          head,
			tail:          head + int64(testDataCapacity),
			lengthIndex:   messageLength * 2,
			lengthValue:   int32(messageLength),
			wantUnblock:   true,
			paddingIndex:  messageLength,
			paddingLength: messageLength,
		},
		unblockGapWithZerosCase(messageLength),
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, buffer := newTestRingBuffer(t, testDataCapacity)
			seedUnblockCase(buffer, testDataCapacity, tc)
			assertUnblockResult(t, ring, buffer, tc)
		})
	}
}

func TestManyToOneRingBuffer_NextCorrelationId(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	if got := ring.NextCorrelationID(); got != 0 {
		t.Fatalf("first id=%d want 0", got)
	}
	if got := ring.NextCorrelationID(); got != 1 {
		t.Fatalf("second id=%d want 1", got)
	}
	if got := ring.NextCorrelationID(); got != 2 {
		t.Fatalf("third id=%d want 2", got)
	}
}

func TestManyToOneRingBuffer_ConsumerHeartbeatTime(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	if ring.ConsumerHeartbeatTime() != 0 {
		t.Fatalf("initial heartbeat=%d want 0", ring.ConsumerHeartbeatTime())
	}
	want := time.Now().UnixMilli()
	ring.SetConsumerHeartbeatTime(want)
	if got := ring.ConsumerHeartbeatTime(); got != want {
		t.Fatalf("heartbeat=%d want %d", got, want)
	}
}

var ringSink any

func TestManyToOneRingBuffer_Write_ZeroAllocs(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	payload := make([]byte, 64)
	src := aeronatomic.Wrap(payload)

	if allocs := testing.AllocsPerRun(1000, func() {
		ok, err := ring.Write(testMsgTypeID, src, 0, len(payload))
		if err != nil || !ok {
			panic("write failed")
		}
		ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {})
	}); allocs != 0 {
		t.Fatalf("Write+Read allocs=%v want 0", allocs)
	}
}

func TestManyToOneRingBuffer_TryClaimCommit_ZeroAllocs(t *testing.T) {
	ring, buffer := newTestRingBuffer(t, testDataCapacity)
	payload := make([]byte, 64)

	if allocs := testing.AllocsPerRun(1000, func() {
		index, err := ring.TryClaim(testMsgTypeID, len(payload))
		if err != nil || index < 0 {
			panic("tryClaim failed")
		}
		buffer.PutBytes(index, payload)
		if err := ring.Commit(index); err != nil {
			panic(err)
		}
		ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {})
	}); allocs != 0 {
		t.Fatalf("TryClaim+Commit+Read allocs=%v want 0", allocs)
	}
}

func TestManyToOneRingBuffer_Read_ZeroAllocs(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	payload := make([]byte, 64)
	src := aeronatomic.Wrap(payload)
	ok, err := ring.Write(testMsgTypeID, src, 0, len(payload))
	if err != nil || !ok {
		t.Fatal("seed write failed")
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		n := ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {})
		ringSink = n
	}); allocs != 0 {
		t.Fatalf("Read allocs=%v want 0", allocs)
	}
}

func TestManyToOneRingBuffer_ControlledRead_ZeroAllocs(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	payload := make([]byte, 64)
	src := aeronatomic.Wrap(payload)
	ok, err := ring.Write(testMsgTypeID, src, 0, len(payload))
	if err != nil || !ok {
		t.Fatal("seed write failed")
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		n := ring.ControlledRead(func(int, aeronatomic.AtomicBuffer, int, int) ringbuffer.ControlledAction {
			return ringbuffer.ControlledContinue
		})
		ringSink = n
	}); allocs != 0 {
		t.Fatalf("ControlledRead allocs=%v want 0", allocs)
	}
}

func TestManyToOneRingBuffer_NextCorrelationId_ZeroAllocs(t *testing.T) {
	ring, _ := newTestRingBuffer(t, testDataCapacity)
	if allocs := testing.AllocsPerRun(1000, func() {
		ringSink = ring.NextCorrelationID()
	}); allocs != 0 {
		t.Fatalf("NextCorrelationId allocs=%v want 0", allocs)
	}
}

func BenchmarkManyToOneRingBuffer_Write(b *testing.B) {
	ring, _ := newTestRingBuffer(b, testDataCapacity)
	payload := make([]byte, 64)
	src := aeronatomic.Wrap(payload)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		for {
			ok, err := ring.Write(testMsgTypeID, src, 0, len(payload))
			if err == nil && ok {
				break
			}
		}
		ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {})
	}
}

func BenchmarkManyToOneRingBuffer_TryClaimCommit(b *testing.B) {
	ring, buffer := newTestRingBuffer(b, testDataCapacity)
	payload := make([]byte, 64)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		for {
			index, err := ring.TryClaim(testMsgTypeID, len(payload))
			if err == nil && index >= 0 {
				buffer.PutBytes(index, payload)
				_ = ring.Commit(index)
				break
			}
		}
		ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {})
	}
}

func BenchmarkManyToOneRingBuffer_Read(b *testing.B) {
	benchmarkManyToOneRead(b, false)
}

func BenchmarkManyToOneRingBuffer_ControlledRead(b *testing.B) {
	benchmarkManyToOneRead(b, true)
}

func benchmarkManyToOneRead(b *testing.B, controlled bool) {
	b.Helper()
	ring, _ := newTestRingBuffer(b, testDataCapacity)
	payload := make([]byte, 64)
	src := aeronatomic.Wrap(payload)
	ok, err := ring.Write(testMsgTypeID, src, 0, len(payload))
	if err != nil || !ok {
		b.Fatal("seed write failed")
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		if controlled {
			ringSink = ring.ControlledRead(func(int, aeronatomic.AtomicBuffer, int, int) ringbuffer.ControlledAction {
				return ringbuffer.ControlledContinue
			})
			continue
		}
		ringSink = ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {})
	}
}

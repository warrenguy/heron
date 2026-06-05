package ringbuffer_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/ringbuffer"
	"pgregory.net/rapid"
)

func rapidWriteMessages(
	t *rapid.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	messageCount, maxLen int,
) ([][]byte, bool) {
	var want [][]byte
	for i := range messageCount {
		msgLen := rapid.IntRange(0, maxLen).Draw(t, fmt.Sprintf("msgLen%d", i))
		payload := rapid.SliceOfN(rapid.Byte(), msgLen, msgLen).Draw(t, fmt.Sprintf("payload%d", i))
		ok, err := ring.Write(testMsgTypeID, atomic.Wrap(payload), 0, len(payload))
		if err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
		if !ok {
			return nil, false
		}
		want = append(want, payload)
	}
	return want, true
}

func rapidWriteReadOnce(t *rapid.T, ring *ringbuffer.ManyToOneRingBuffer, maxLen int) bool {
	msgLen := rapid.IntRange(0, maxLen).Draw(t, "msgLen")
	payload := rapid.SliceOfN(rapid.Byte(), msgLen, msgLen).Draw(t, "payload")

	ok, err := ring.Write(testMsgTypeID, atomic.Wrap(payload), 0, len(payload))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !ok {
		return false
	}

	var got []byte
	n := ring.Read(func(_ int, buffer atomic.AtomicBuffer, index, length int) {
		got = readPayload(buffer, index, length)
	})
	if n != 1 {
		t.Fatalf("messages=%d want 1", n)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
	assertConsumerCaughtUp(t, ring)
	return true
}

func TestManyToOneRingBuffer_WriteRead_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dataCapacity := 1 << rapid.IntRange(4, 14).Draw(t, "shift")
		ring, _ := newRingBufferForTest(t, dataCapacity)
		maxLen := clampMaxMsgLen(ring.MaxMsgLength(), 1)
		rapidWriteReadOnce(t, ring, maxLen)
	})
}

func TestManyToOneRingBuffer_NextCorrelationId_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		steps := rapid.IntRange(1, 32).Draw(t, "steps")
		ring, _ := newRingBufferForTest(t, testDataCapacity)

		var want int64
		for range steps {
			if got := ring.NextCorrelationID(); got != want {
				t.Fatalf("id=%d want %d", got, want)
			}
			want++
		}
	})
}

func TestManyToOneRingBuffer_writeReadDrain_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dataCapacity := 1 << rapid.IntRange(4, 12).Draw(t, "shift")
		ring, _ := newRingBufferForTest(t, dataCapacity)
		maxLen := clampMaxMsgLen(ring.MaxMsgLength(), 0)

		messageCount := rapid.IntRange(1, 16).Draw(t, "messageCount")
		want, ok := rapidWriteMessages(t, ring, messageCount, maxLen)
		if !ok {
			return
		}

		got := drainReadMessages(ring)
		comparePayloadSlices(t, got, want)
		assertConsumerCaughtUp(t, ring)
	})
}

func TestManyToOneRingBuffer_tryClaimLifecycle_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ring, buffer := newRingBufferForTest(t, testDataCapacity)
		maxLen := clampMaxMsgLen(ring.MaxMsgLength(), 1)

		claimCount := rapid.IntRange(1, 8).Draw(t, "claimCount")
		var wantCommitted [][]byte
		var soleCommitIndex int
		for i := range claimCount {
			length := rapid.IntRange(1, maxLen).Draw(t, fmt.Sprintf("claimLen%d", i))
			commit := rapid.Bool().Draw(t, fmt.Sprintf("commit%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("claimPayload%d", i))

			index, err := ring.TryClaim(testMsgTypeID, length)
			if err != nil {
				t.Fatalf("TryClaim[%d]: %v", i, err)
			}
			if index == ringbuffer.InsufficientCapacity {
				return
			}

			buffer.PutBytes(index, payload)
			if commit {
				if err := ring.Commit(index); err != nil {
					t.Fatalf("Commit[%d]: %v", i, err)
				}
				wantCommitted = append(wantCommitted, payload)
				soleCommitIndex = index
			} else if err := ring.Abort(index); err != nil {
				t.Fatalf("Abort[%d]: %v", i, err)
			}
		}

		if claimCount == 1 && len(wantCommitted) == 1 && rapid.Bool().Draw(t, "doubleCommit") {
			if err := ring.Commit(soleCommitIndex); err == nil {
				t.Fatal("expected error on second commit")
			}
		}

		got := drainReadMessages(ring)
		if len(got) != len(wantCommitted) {
			t.Fatalf("messages=%d want %d", len(got), len(wantCommitted))
		}
		comparePayloadSlices(t, got, wantCommitted)
		assertConsumerCaughtUp(t, ring)
	})
}

func TestManyToOneRingBuffer_controlledCommitAtEnd_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dataCapacity := 1 << rapid.IntRange(6, 9).Draw(t, "shift")
		ring, buffer := newRingBufferForTest(t, dataCapacity)
		maxLen := ring.MaxMsgLength()
		cap := min(maxLen, 8)
		if cap < 1 {
			return
		}
		msgLen := rapid.IntRange(1, cap).Draw(t, "msgLen")
		seedCommitAtEndCase(buffer, dataCapacity, msgLen)

		n := ring.ControlledRead(func(int, atomic.AtomicBuffer, int, int) ringbuffer.ControlledAction {
			return ringbuffer.ControlledCommit
		})
		if n != 1 {
			t.Fatalf("messages=%d want 1", n)
		}
		assertConsumerCaughtUp(t, ring)

		n = ring.Read(func(int, atomic.AtomicBuffer, int, int) {
			t.Fatal("unexpected message after commit at buffer end")
		})
		if n != 0 {
			t.Fatalf("second read messages=%d want 0", n)
		}
	})
}

func TestManyToOneRingBuffer_readLimitNonPositive_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ring, _ := newRingBufferForTest(t, testDataCapacity)
		limit := rapid.SampledFrom([]int{-1, 0}).Draw(t, "limit")
		maxLen := clampMaxMsgLen(ring.MaxMsgLength(), 1)
		msgLen := rapid.IntRange(1, maxLen).Draw(t, "msgLen")
		payload := rapid.SliceOfN(rapid.Byte(), msgLen, msgLen).Draw(t, "payload")

		ok, err := ring.Write(testMsgTypeID, atomic.Wrap(payload), 0, len(payload))
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		if !ok {
			return
		}
		consumerBefore := ring.ConsumerPosition()

		called := false
		if n := ring.ReadWithLimit(func(int, atomic.AtomicBuffer, int, int) {
			called = true
		}, limit); n != 0 || called {
			t.Fatalf("messages=%d called=%v want 0/false", n, called)
		}
		if ring.ConsumerPosition() != consumerBefore {
			t.Fatalf("consumer=%d want %d", ring.ConsumerPosition(), consumerBefore)
		}
	})
}

func TestManyToOneRingBuffer_UnblockGap_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		messageLength := unblockMessageLength
		shift := rapid.IntRange(4, 12).Draw(t, "shift")
		dataCapacity := 1 << shift
		for dataCapacity < messageLength*4 {
			shift++
			dataCapacity = 1 << shift
		}

		ring, atomicBuffer := newRingBufferForTest(t, dataCapacity)

		tc := unblockGapWithZerosCase(messageLength)
		seedUnblockCase(atomicBuffer, dataCapacity, tc)
		assertUnblockResult(t, ring, atomicBuffer, tc)
	})
}

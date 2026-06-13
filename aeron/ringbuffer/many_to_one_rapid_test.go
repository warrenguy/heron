//go:build rapid

package ringbuffer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/ringbuffer"
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
		require.NoError(t, err)
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
	require.NoError(t, err)
	if !ok {
		return false
	}

	var got []byte
	n := ring.Read(func(_ int, buffer atomic.AtomicBuffer, index, length int) {
		got = readPayload(buffer, index, length)
	})
	require.Equal(t, 1, n)
	require.Equal(t, payload, got)
	assertConsumerCaughtUp(t, ring)
	return true
}

func TestManyToOneRingBuffer_WriteRead_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dataCapacity := 1 << rapid.IntRange(4, 14).Draw(t, "shift")
		ring, _ := newTestRingBuffer(t, dataCapacity)
		maxLen := clampMaxMsgLen(ring.MaxMsgLength(), 1)
		rapidWriteReadOnce(t, ring, maxLen)
	})
}

func TestManyToOneRingBuffer_NextCorrelationId_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		steps := rapid.IntRange(1, 32).Draw(t, "steps")
		ring, _ := newTestRingBuffer(t, testDataCapacity)

		var want int64
		for range steps {
			got := ring.NextCorrelationID()
			require.Equal(t, want, got)
			want++
		}
	})
}

func TestManyToOneRingBuffer_writeReadDrain_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dataCapacity := 1 << rapid.IntRange(4, 12).Draw(t, "shift")
		ring, _ := newTestRingBuffer(t, dataCapacity)
		maxLen := clampMaxMsgLen(ring.MaxMsgLength(), 0)

		messageCount := rapid.IntRange(1, 16).Draw(t, "messageCount")
		want, ok := rapidWriteMessages(t, ring, messageCount, maxLen)
		if !ok {
			return
		}

		got := drainReadMessages(ring)
		require.Equal(t, want, got)
		assertConsumerCaughtUp(t, ring)
	})
}

func TestManyToOneRingBuffer_tryClaimLifecycle_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ring, buffer := newTestRingBuffer(t, testDataCapacity)
		maxLen := clampMaxMsgLen(ring.MaxMsgLength(), 1)

		claimCount := rapid.IntRange(1, 8).Draw(t, "claimCount")
		var wantCommitted [][]byte
		var soleCommitIndex int
		for i := range claimCount {
			length := rapid.IntRange(1, maxLen).Draw(t, fmt.Sprintf("claimLen%d", i))
			commit := rapid.Bool().Draw(t, fmt.Sprintf("commit%d", i))
			payload := rapid.SliceOfN(rapid.Byte(), length, length).Draw(t, fmt.Sprintf("claimPayload%d", i))

			index, err := ring.TryClaim(testMsgTypeID, length)
			require.NoError(t, err)
			if index == ringbuffer.InsufficientCapacity {
				return
			}

			buffer.PutBytes(index, payload)
			if commit {
				err := ring.Commit(index)
				require.NoError(t, err)
				wantCommitted = append(wantCommitted, payload)
				soleCommitIndex = index
			} else {
				err := ring.Abort(index)
				require.NoError(t, err)
			}
		}

		if claimCount == 1 && len(wantCommitted) == 1 && rapid.Bool().Draw(t, "doubleCommit") {
			err := ring.Commit(soleCommitIndex)
			require.Error(t, err)
		}

		got := drainReadMessages(ring)
		require.Equal(t, wantCommitted, got)
		assertConsumerCaughtUp(t, ring)
	})
}

func TestManyToOneRingBuffer_controlledCommitAtEnd_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dataCapacity := 1 << rapid.IntRange(6, 9).Draw(t, "shift")
		ring, buffer := newTestRingBuffer(t, dataCapacity)
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
		require.Equal(t, 1, n)
		assertConsumerCaughtUp(t, ring)

		n = ring.Read(func(int, atomic.AtomicBuffer, int, int) {
			require.FailNow(t, "unexpected message after commit at buffer end")
		})
		require.Equal(t, 0, n)
	})
}

func TestManyToOneRingBuffer_readLimitNonPositive_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ring, _ := newTestRingBuffer(t, testDataCapacity)
		limit := rapid.SampledFrom([]int{-1, 0}).Draw(t, "limit")
		maxLen := clampMaxMsgLen(ring.MaxMsgLength(), 1)
		msgLen := rapid.IntRange(1, maxLen).Draw(t, "msgLen")
		payload := rapid.SliceOfN(rapid.Byte(), msgLen, msgLen).Draw(t, "payload")

		ok, err := ring.Write(testMsgTypeID, atomic.Wrap(payload), 0, len(payload))
		require.NoError(t, err)
		if !ok {
			return
		}
		consumerBefore := ring.ConsumerPosition()

		called := false
		n := ring.ReadWithLimit(func(int, atomic.AtomicBuffer, int, int) {
			called = true
		}, limit)
		require.Equal(t, 0, n)
		require.False(t, called)
		require.Equal(t, consumerBefore, ring.ConsumerPosition())
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

		ring, atomicBuffer := newTestRingBuffer(t, dataCapacity)

		tc := unblockGapWithZerosCase(messageLength)
		seedUnblockCase(atomicBuffer, dataCapacity, tc)
		assertUnblockResult(t, ring, atomicBuffer, tc)
	})
}

//go:build rapid

package broadcast_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
	"github.com/warrenguy/heron/record"
)

func TestBroadcast_rapid_transmitReceiveOrder(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		ab, tx, rx := rapidBroadcastPair(t, capacity)

		n := rapid.IntRange(1, 32).Draw(t, "messageCount")
		for i := range n {
			payloadLen := rapid.IntRange(1, tx.MaxMsgLength()).Draw(t, "payloadLen")
			want := rapidPayload(t, payloadLen)
			rapidTransmit(t, tx, want)
			assertTailIntentOrdering(t, ab, capacity)
			rapidReceiveAndAssertPayload(t, rx, want, i)
		}
		require.False(t, rx.ReceiveNext())
		require.Zero(t, rx.LappedCount())
	})
}

func TestBroadcast_rapid_tailIntentOrdering(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		ab, tx, _ := rapidBroadcastPair(t, capacity)

		n := rapid.IntRange(1, 64).Draw(t, "messageCount")
		for range n {
			payloadLen := rapid.IntRange(1, tx.MaxMsgLength()).Draw(t, "payloadLen")
			src := rapidPayload(t, payloadLen)
			rapidTransmit(t, tx, src)
			assertTailIntentOrdering(t, ab, capacity)
		}
	})
}

func TestBroadcast_rapid_lapVsNoLap(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		_, tx, rx := rapidBroadcastPair(t, capacity)

		src := testPayload8
		payloadLen := len(src)

		stall := rapid.Bool().Draw(t, "stallReceiver")
		writes := rapid.IntRange(1, writesToLap(capacity, payloadLen, 4)).Draw(t, "writes")
		if stall {
			writes = rapid.IntRange(
				writesToLap(capacity, payloadLen, 2),
				writesToLap(capacity, payloadLen, 8),
			).Draw(t, "writesStall")
		}

		for range writes {
			rapidTransmit(t, tx, src)
			if !stall {
				rapidReceiveValid(t, rx, "while keeping up")
			}
		}

		if stall {
			require.True(t, rx.ReceiveNext())
			require.True(t, rx.Validate())
			require.Positive(t, rx.LappedCount())
		} else {
			require.Zero(t, rx.LappedCount())
		}
	})
}

func TestBroadcast_rapid_noTornReads(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		_, tx, rx := rapidBroadcastPair(t, capacity)

		n := rapid.IntRange(1, 24).Draw(t, "messageCount")
		for range n {
			payloadLen := rapid.IntRange(1, tx.MaxMsgLength()).Draw(t, "payloadLen")
			rapidTransmit(t, tx, rapidPayload(t, payloadLen))
		}

		validated := 0
		for rx.ReceiveNext() {
			if !rx.Validate() {
				continue
			}
			validated++
			assertNoTornRead(t, rx)
		}
		require.Positive(t, validated)
	})
}

func TestBroadcast_rapid_paddingWrap(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		// Smaller capacities increase wrap/padding frequency.
		shift := rapid.IntRange(6, 9).Draw(t, "shift")
		capacity := 1 << shift
		_, tx, rx := rapidBroadcastPair(t, capacity)

		smallLen := rapid.IntRange(1, tx.MaxMsgLength()).Draw(t, "smallLen")
		small := make([]byte, smallLen)
		recordAligned := alignedRecordLength(int(smallLen))
		if recordAligned >= capacity {
			t.Skip("record larger than capacity")
		}

		maxBefore := messagesBeforeWrap(capacity, int(smallLen)) + 1
		beforeWrap := rapid.IntRange(1, maxBefore).Draw(t, "beforeWrap")
		for range beforeWrap {
			rapidTransmit(t, tx, small)
			rapidReceiveValid(t, rx, "for small message")
		}

		largeLen := rapid.IntRange(tx.MaxMsgLength()/2, tx.MaxMsgLength()).Draw(t, "largeLen")
		large := make([]byte, largeLen)
		for i := range large {
			large[i] = byte(i)
		}
		rapidTransmit(t, tx, large)
		rapidReceiveValid(t, rx, "after wrap")
		require.Equal(t, testMsgTypeID, rx.TypeID())
		require.Equal(t, largeLen, rx.Length())
		got := copyPayload(rx)
		require.Equal(t, large, got)
	})
}

func TestBroadcast_rapid_interleavedSchedule(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		_, tx, rx := rapidBroadcastPair(t, capacity)

		steps := rapid.IntRange(1, 40).Draw(t, "steps")
		var queue [][]byte
		var queueMaxLen int

		for range steps {
			nextLen := rapid.IntRange(1, tx.MaxMsgLength()).Draw(t, "payloadLen")
			maxPlen := max(queueMaxLen, nextLen)
			forceReceive := len(queue) >= messagesBeforeWrap(capacity, maxPlen)
			if len(queue) > 0 && (rapid.Bool().Draw(t, "receive") || forceReceive) {
				queue, queueMaxLen = rapidDrainQueueHead(t, rx, queue, "on interleaved receive")
				continue
			}

			want := rapidPayload(t, nextLen)
			rapidTransmit(t, tx, want)
			queue = append(queue, want)
			if nextLen > queueMaxLen {
				queueMaxLen = nextLen
			}
		}

		for len(queue) > 0 {
			queue, _ = rapidDrainQueueHead(t, rx, queue, "on final drain")
		}
		require.False(t, rx.ReceiveNext())
		require.Zero(t, rx.LappedCount())
	})
}

func TestBroadcast_rapid_dualReceiver(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		tx, fast, slow := rapidDualReceiverSetup(t, capacity)

		src := testPayload8
		payloadLen := len(src)
		writes := rapid.IntRange(
			writesToLap(capacity, payloadLen, 2),
			writesToLap(capacity, payloadLen, 8),
		).Draw(t, "writes")

		for range writes {
			rapidTransmit(t, tx, src)
			rapidReceiveValid(t, fast, "fast receiver fell behind transmit")
		}

		require.True(t, slow.ReceiveNext())
		require.True(t, slow.Validate())
		require.Positive(t, slow.LappedCount())
		require.Zero(t, fast.LappedCount())
	})
}

func TestBroadcast_rapid_lappedCountMonotonic(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		_, tx, rx := rapidBroadcastPair(t, capacity)

		src := testPayload8
		prevLapped := rx.LappedCount()
		stallRemaining := 0
		var txCount, rxCount, unread int

		steps := rapid.IntRange(1, 40).Draw(t, "steps")
		for range steps {
			if stallRemaining > 0 {
				rapidTransmit(t, tx, src)
				stallRemaining--
				txCount++
				unread++
			} else if unread > 0 && rapid.Bool().Draw(t, "receive") {
				if rx.ReceiveNext() {
					if rx.LappedCount() > prevLapped {
						unread = 0
					} else {
						unread--
					}
					rxCount++
				}
			} else {
				if rapid.Bool().Draw(t, "startStallBurst") {
					stallRemaining = rapid.IntRange(2, 8).Draw(t, "stallBurst")
				}
				rapidTransmit(t, tx, src)
				txCount++
				unread++
			}

			lapped := rx.LappedCount()
			require.GreaterOrEqual(t, lapped, prevLapped)
			prevLapped = lapped
		}

		keptUp := stallRemaining == 0 && txCount == rxCount && unread == 0
		if keptUp {
			require.Zero(t, rx.LappedCount())
		}
	})
}

func TestBroadcast_rapid_validateImpliesReadable(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		_, tx, rx := rapidBroadcastPair(t, capacity)

		n := rapid.IntRange(1, 32).Draw(t, "messageCount")
		for range n {
			payloadLen := rapid.IntRange(1, tx.MaxMsgLength()).Draw(t, "payloadLen")
			rapidTransmit(t, tx, rapidPayload(t, payloadLen))
		}

		validated := 0
		for rx.ReceiveNext() {
			if !rx.Validate() {
				continue
			}
			validated++
			require.Equal(t, testMsgTypeID, rx.TypeID())
			require.Positive(t, rx.Length())
			assertNoTornRead(t, rx)
			got := copyPayload(rx)
			require.Len(t, got, rx.Length())
		}
		require.Positive(t, validated)
	})
}

func TestBroadcast_rapid_tornReadResync(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		buf, tx, rx := rapidBroadcastPair(t, capacity)
		sequence := 0

		// Phase: records before the tear, received in step.
		inStepCount := rapid.IntRange(0, 32).Draw(t, "inStepCount")
		for range inStepCount {
			rapidTransmitSequenced(t, tx, &sequence)
			rapidReceiveSequenced(t, rx, sequence-1)
		}

		// Phase: ReceiveNext loads a torn header, then the slot laps.
		tornLength := rapid.Int32().Draw(t, "tornLength")
		tornTypeID := rapid.SampledFrom([]int32{
			broadcast.PaddingMsgTypeID,
			testMsgTypeID,
			math.MaxInt32,
		}).Draw(t, "tornTypeID")
		rapidTransmitSequenced(t, tx, &sequence)
		slotPosition := rx.NextRecordForTest()
		slotOffset := int(slotPosition) & (capacity - 1)
		buf.PutInt32(record.LengthOffset(slotOffset), tornLength)
		buf.PutInt32(record.TypeOffset(slotOffset), tornTypeID)
		require.True(t, rx.ReceiveNext())
		for buf.GetInt64Volatile(broadcast.TailIntentIndex(capacity)) < slotPosition+int64(capacity) {
			rapidTransmitSequenced(t, tx, &sequence)
		}
		require.False(t, rx.Validate())

		// Phase: Resync resumes at the latest record.
		lappedCount := rx.LappedCount()
		rx.Resync()
		require.Equal(t, lappedCount+1, rx.LappedCount())
		latestSequence := sequence - 1
		laterCount := rapid.IntRange(0, min(32, capacity/16-2)).Draw(t, "laterCount")
		for range laterCount {
			rapidTransmitSequenced(t, tx, &sequence)
		}
		for want := latestSequence; want < sequence; want++ {
			rapidReceiveSequenced(t, rx, want)
			require.Equal(t, lappedCount+1, rx.LappedCount())
		}
		require.False(t, rx.ReceiveNext())
	})
}

func rapidCapacity(t *rapid.T) int {
	t.Helper()
	shift := rapid.IntRange(6, 12).Draw(t, "shift")
	return 1 << shift
}

func rapidPayload(t *rapid.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rapid.Byte().Draw(t, "payloadByte"))
	}
	return b
}

func rapidBroadcastPair(
	t *rapid.T,
	capacity int,
) (atomicbuffer.AtomicBuffer, *broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver) {
	t.Helper()
	ab := newBufferWithDataCapacity(capacity)
	tx, rx, err := newBroadcastTXRX(ab)
	require.NoError(t, err)
	return ab, tx, rx
}

func rapidDualReceiverSetup(
	t *rapid.T,
	capacity int,
) (*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver, *broadcast.BroadcastReceiver) {
	t.Helper()
	ab := newBufferWithDataCapacity(capacity)
	tx, fast, err := newBroadcastTXRX(ab)
	require.NoError(t, err)
	slow, err := broadcast.NewBroadcastReceiver(ab)
	require.NoError(t, err)
	return tx, fast, slow
}

func rapidTransmit(t *rapid.T, tx *broadcast.BroadcastTransmitter, payload []byte) {
	t.Helper()
	err := tx.Transmit(testMsgTypeID, payload)
	require.NoError(t, err)
}

func rapidReceiveValid(t *rapid.T, rx *broadcast.BroadcastReceiver, context string) {
	t.Helper()
	require.Truef(t, rx.ReceiveNext(), "%s", context)
	require.Truef(t, rx.Validate(), "%s", context)
}

func rapidReceiveAndAssertPayload(
	t *rapid.T,
	rx *broadcast.BroadcastReceiver,
	want []byte,
	msgIndex int,
) {
	t.Helper()
	rapidReceiveValid(t, rx, fmt.Sprintf("message %d", msgIndex))
	assertNoTornRead(t, rx)
	got := copyPayload(rx)
	require.Equal(t, want, got)
	require.Equal(t, testMsgTypeID, rx.TypeID())
}

func rapidDrainQueueHead(
	t *rapid.T,
	rx *broadcast.BroadcastReceiver,
	queue [][]byte,
	context string,
) ([][]byte, int) {
	t.Helper()
	rapidReceiveValid(t, rx, context)
	got := copyPayload(rx)
	require.Equal(t, queue[0], got)
	queue = queue[1:]
	if len(queue) == 0 {
		return queue, 0
	}
	maxLen := 0
	for _, p := range queue {
		if len(p) > maxLen {
			maxLen = len(p)
		}
	}
	return queue, maxLen
}

// rapidTransmitSequenced transmits one record whose 8-byte body holds
// sequence, then advances sequence.
func rapidTransmitSequenced(t *rapid.T, tx *broadcast.BroadcastTransmitter, sequence *int) {
	t.Helper()
	rapidTransmit(t, tx, binary.LittleEndian.AppendUint64(nil, uint64(*sequence)))
	*sequence++
}

// rapidReceiveSequenced receives one valid record. Requires its body to hold
// want.
func rapidReceiveSequenced(t *rapid.T, rx *broadcast.BroadcastReceiver, want int) {
	t.Helper()
	rapidReceiveAndAssertPayload(t, rx, binary.LittleEndian.AppendUint64(nil, uint64(want)), want)
}

func assertTailIntentOrdering(t *rapid.T, ab atomicbuffer.AtomicBuffer, capacity int) {
	t.Helper()
	tailIntent := ab.GetInt64Volatile(broadcast.TailIntentIndex(capacity))
	tail := ab.GetInt64Volatile(broadcast.TailIndex(capacity))
	require.GreaterOrEqual(t, tailIntent, tail)
}

func assertNoTornRead(t *rapid.T, rx *broadcast.BroadcastReceiver) {
	t.Helper()
	typeID1 := rx.TypeID()
	off1 := rx.Offset()
	len1 := rx.Length()
	payload1 := copyPayload(rx)

	typeID2 := rx.TypeID()
	off2 := rx.Offset()
	len2 := rx.Length()
	payload2 := copyPayload(rx)

	require.True(t, typeID1 == typeID2 && off1 == off2 && len1 == len2, "metadata torn")
	require.Equal(t, payload1, payload2)
}

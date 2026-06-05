package broadcast_test

import (
	"bytes"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"pgregory.net/rapid"
)

func TestBroadcast_rapid_transmitReceiveOrder(t *testing.T) {
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
		if rx.ReceiveNext() {
			t.Fatal("unexpected extra message")
		}
		if rx.LappedCount() != 0 {
			t.Fatalf("lappedCount=%d want 0 when keeping up", rx.LappedCount())
		}
	})
}

func TestBroadcast_rapid_tailIntentOrdering(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		ab, tx := rapidTransmitter(t, capacity)

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
			if !rx.ReceiveNext() {
				t.Fatal("expected catch-up receive after stall")
			}
			if !rx.Validate() {
				t.Fatal("expected record to validate after catch-up")
			}
			if rx.LappedCount() == 0 {
				t.Fatal("expected lappedCount > 0 after stall")
			}
		} else if rx.LappedCount() != 0 {
			t.Fatalf("lappedCount=%d want 0 when not stalling", rx.LappedCount())
		}
	})
}

func TestBroadcast_rapid_noTornReads(t *testing.T) {
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
		if validated == 0 {
			t.Fatal("expected at least one validated record")
		}
	})
}

func TestBroadcast_rapid_paddingWrap(t *testing.T) {
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
		if rx.TypeID() != testMsgTypeID {
			t.Fatalf("typeId=%d want %d", rx.TypeID(), testMsgTypeID)
		}
		if rx.Length() != largeLen {
			t.Fatalf("length=%d want %d", rx.Length(), largeLen)
		}
		got := copyPayload(rx)
		if !bytes.Equal(got, large) {
			t.Fatal("large payload mismatch after padding wrap")
		}
	})
}

func TestBroadcast_rapid_interleavedSchedule(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapidCapacity(t)
		_, tx, rx := rapidBroadcastPair(t, capacity)

		steps := rapid.IntRange(1, 40).Draw(t, "steps")
		var queue [][]byte
		var queueMaxLen int

		for range steps {
			nextLen := rapid.IntRange(1, tx.MaxMsgLength()).Draw(t, "payloadLen")
			maxPlen := nextLen
			if queueMaxLen > maxPlen {
				maxPlen = queueMaxLen
			}
			forceReceive := len(queue) >= messagesBeforeWrap(capacity, maxPlen)
			if len(queue) > 0 && (rapid.Bool().Draw(t, "receive") || forceReceive) {
				queue, queueMaxLen = rapidDrainQueueHead(t, rx, queue, queueMaxLen, "on interleaved receive")
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
			queue, queueMaxLen = rapidDrainQueueHead(t, rx, queue, queueMaxLen, "on final drain")
		}
		if rx.ReceiveNext() {
			t.Fatal("unexpected extra message after drain")
		}
		if rx.LappedCount() != 0 {
			t.Fatalf("lappedCount=%d want 0 when never falling behind", rx.LappedCount())
		}
	})
}

func TestBroadcast_rapid_dualReceiver(t *testing.T) {
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

		if !slow.ReceiveNext() {
			t.Fatal("slow receiver expected catch-up receive")
		}
		if !slow.Validate() {
			t.Fatal("slow receiver expected record to validate after catch-up")
		}
		if slow.LappedCount() == 0 {
			t.Fatal("slow receiver expected lappedCount > 0")
		}
		if fast.LappedCount() != 0 {
			t.Fatalf("fast lappedCount=%d want 0", fast.LappedCount())
		}
	})
}

func TestBroadcast_rapid_lappedCountMonotonic(t *testing.T) {
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
			if lapped < prevLapped {
				t.Fatalf("lappedCount decreased: %d -> %d", prevLapped, lapped)
			}
			prevLapped = lapped
		}

		keptUp := stallRemaining == 0 && txCount == rxCount && unread == 0
		if keptUp && rx.LappedCount() != 0 {
			t.Fatalf("lappedCount=%d want 0 when keeping up", rx.LappedCount())
		}
	})
}

func TestBroadcast_rapid_validateImpliesReadable(t *testing.T) {
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
			if rx.TypeID() != testMsgTypeID {
				t.Fatalf("typeId=%d want %d", rx.TypeID(), testMsgTypeID)
			}
			if rx.Length() <= 0 {
				t.Fatalf("length=%d want > 0", rx.Length())
			}
			assertNoTornRead(t, rx)
			got := copyPayload(rx)
			if len(got) != rx.Length() {
				t.Fatalf("payload len=%d want length=%d", len(got), rx.Length())
			}
		}
		if validated == 0 {
			t.Fatal("expected at least one validated record")
		}
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
) (atomic.AtomicBuffer, *broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver) {
	t.Helper()
	ab := newBufferWithDataCapacity(capacity)
	tx, rx, err := newBroadcastTXRX(ab)
	if err != nil {
		t.Fatalf("broadcast pair: %v", err)
	}
	return ab, tx, rx
}

func rapidTransmitter(
	t *rapid.T,
	capacity int,
) (atomic.AtomicBuffer, *broadcast.BroadcastTransmitter) {
	t.Helper()
	ab, tx, _ := rapidBroadcastPair(t, capacity)
	return ab, tx
}

func rapidDualReceiverSetup(
	t *rapid.T,
	capacity int,
) (*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver, *broadcast.BroadcastReceiver) {
	t.Helper()
	ab := newBufferWithDataCapacity(capacity)
	tx, fast, err := newBroadcastTXRX(ab)
	if err != nil {
		t.Fatalf("broadcast pair: %v", err)
	}
	slow, err := broadcast.NewBroadcastReceiver(ab)
	if err != nil {
		t.Fatalf("slow receiver: %v", err)
	}
	return tx, fast, slow
}

func rapidTransmit(t *rapid.T, tx *broadcast.BroadcastTransmitter, payload []byte) {
	t.Helper()
	if err := tx.Transmit(testMsgTypeID, payload); err != nil {
		t.Fatalf("Transmit: %v", err)
	}
}

func rapidReceiveValid(t *rapid.T, rx *broadcast.BroadcastReceiver, context string) {
	t.Helper()
	if !rx.ReceiveNext() {
		t.Fatalf("%s: ReceiveNext=false", context)
	}
	if !rx.Validate() {
		t.Fatalf("%s: Validate=false", context)
	}
}

func rapidReceiveAndAssertPayload(
	t *rapid.T,
	rx *broadcast.BroadcastReceiver,
	want []byte,
	msgIndex int,
) {
	t.Helper()
	if !rx.ReceiveNext() {
		t.Fatalf("message %d: ReceiveNext=false", msgIndex)
	}
	if !rx.Validate() {
		t.Fatal("expected valid record while keeping up")
	}
	assertNoTornRead(t, rx)
	got := copyPayload(rx)
	if !bytes.Equal(got, want) {
		t.Fatalf("message %d: payload mismatch", msgIndex)
	}
	if rx.TypeID() != testMsgTypeID {
		t.Fatalf("message %d: typeId=%d", msgIndex, rx.TypeID())
	}
}

func rapidDrainQueueHead(
	t *rapid.T,
	rx *broadcast.BroadcastReceiver,
	queue [][]byte,
	queueMaxLen int,
	context string,
) ([][]byte, int) {
	t.Helper()
	if !rx.ReceiveNext() {
		t.Fatalf("ReceiveNext=false %s", context)
	}
	if !rx.Validate() {
		t.Fatalf("Validate=false %s", context)
	}
	got := copyPayload(rx)
	if !bytes.Equal(got, queue[0]) {
		t.Fatalf("payload mismatch %s", context)
	}
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

func assertTailIntentOrdering(t *rapid.T, ab atomic.AtomicBuffer, capacity int) {
	t.Helper()
	tailIntent := ab.GetInt64Volatile(broadcast.TailIntentIndex(capacity))
	tail := ab.GetInt64Volatile(broadcast.TailIndex(capacity))
	if tailIntent < tail {
		t.Fatalf("tailIntent=%d < tail=%d", tailIntent, tail)
	}
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

	if typeID1 != typeID2 || off1 != off2 || len1 != len2 {
		t.Fatalf("metadata torn: (%d,%d,%d) vs (%d,%d,%d)", typeID1, off1, len1, typeID2, off2, len2)
	}
	if !bytes.Equal(payload1, payload2) {
		t.Fatal("payload torn across reads")
	}
}

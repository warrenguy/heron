package broadcast_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/broadcast"
)

func warmTransmit(tb testing.TB, tx *broadcast.BroadcastTransmitter) {
	tb.Helper()
	if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
		tb.Fatal(err)
	}
}

func warmTransmitReceive(tb testing.TB, tx *broadcast.BroadcastTransmitter, rx *broadcast.BroadcastReceiver) {
	tb.Helper()
	warmTransmit(tb, tx)
	if !rx.ReceiveNext() {
		tb.Fatal("warm-up receive failed")
	}
}

func assertZeroAllocs(tb testing.TB, label string, n int, fn func()) {
	tb.Helper()
	if allocs := testing.AllocsPerRun(n, fn); allocs != 0 {
		tb.Fatalf("%s allocs = %v, want 0", label, allocs)
	}
}

func TestBroadcastReceiver_ReceiveNext_ZeroAllocs(t *testing.T) {
	tx, rx, _ := newTestBroadcastPair(t)
	warmTransmit(t, tx)

	assertZeroAllocs(t, "ReceiveNext", 1000, func() {
		if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
			t.Fatal(err)
		}
		if !rx.ReceiveNext() {
			t.Fatal("ReceiveNext=false")
		}
	})
}

func TestBroadcastReceiver_Validate_ZeroAllocs(t *testing.T) {
	tx, rx, _ := newTestBroadcastPair(t)
	warmTransmitReceive(t, tx, rx)

	assertZeroAllocs(t, "Validate", 1000, func() {
		_ = rx.Validate()
	})
}

func TestBroadcastReceiver_TypeID_Offset_Length_ZeroAllocs(t *testing.T) {
	tx, rx, _ := newTestBroadcastPair(t)
	warmTransmitReceive(t, tx, rx)

	assertZeroAllocs(t, "TypeID/Offset/Length", 1000, func() {
		_ = rx.TypeID()
		_ = rx.Offset()
		_ = rx.Length()
	})
}

func TestBroadcastTransmitter_Transmit_ZeroAllocs(t *testing.T) {
	tx, _, _ := newTestBroadcastPair(t)
	warmTransmit(t, tx)

	assertZeroAllocs(t, "Transmit", 1000, func() {
		if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
			t.Fatal(err)
		}
	})
}

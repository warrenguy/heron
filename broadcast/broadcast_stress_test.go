//go:build stress

package broadcast_test

import (
	"testing"
)

func TestBroadcastStress_FastReceiver(t *testing.T) {
	tx, rx, _ := newTestBroadcastPair(t)

	const iterations = 50_000
	src := testPayload8
	for range iterations {
		if err := tx.Transmit(testMsgTypeID, src); err != nil {
			t.Fatal(err)
		}
		if !rx.ReceiveNext() {
			t.Fatal("ReceiveNext=false")
		}
		if !rx.Validate() {
			t.Fatal("Validate=false")
		}
	}
	if rx.LappedCount() != 0 {
		t.Fatalf("lappedCount=%d want 0", rx.LappedCount())
	}
}

func TestBroadcastStress_SlowReceiver(t *testing.T) {
	ab := newTestBuffer(t)
	tx := newTransmitter(t, ab)
	rx := newReceiver(t, ab)

	src := testPayload8
	overflow := writesToLap(testCapacity, len(src), 8)

	for range overflow {
		if err := tx.Transmit(testMsgTypeID, src); err != nil {
			t.Fatal(err)
		}
	}

	if !rx.ReceiveNext() {
		t.Fatal("expected catch-up receive")
	}
	if !rx.Validate() {
		t.Fatal("expected record to validate after catch-up")
	}
	if rx.LappedCount() == 0 {
		t.Fatal("expected lappedCount > 0 for slow receiver")
	}
}

func TestBroadcastStress_FastAndSlowReceivers(t *testing.T) {
	ab := newTestBuffer(t)
	tx := newTransmitter(t, ab)
	fast := newReceiver(t, ab)
	slow := newReceiver(t, ab)

	src := testPayload8
	overflow := writesToLap(testCapacity, len(src), 4)

	for range overflow {
		if err := tx.Transmit(testMsgTypeID, src); err != nil {
			t.Fatal(err)
		}
		if !fast.ReceiveNext() {
			t.Fatal("fast receiver fell behind transmit")
		}
		if !fast.Validate() {
			t.Fatal("fast receiver record invalid while keeping up")
		}
	}

	if !slow.ReceiveNext() {
		t.Fatal("slow receiver expected catch-up receive")
	}
	if slow.LappedCount() == 0 {
		t.Fatal("slow receiver expected lappedCount > 0")
	}
	if fast.LappedCount() != 0 {
		t.Fatalf("fast lappedCount=%d want 0", fast.LappedCount())
	}
}

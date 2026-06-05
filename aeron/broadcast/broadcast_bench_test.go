package broadcast_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/broadcast"
)

func benchmarkBroadcastWarm(
	b *testing.B,
) (*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver) {
	b.Helper()
	tx, rx, _ := newTestBroadcastPair(b)
	const warm = 1024
	for range warm {
		if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
			b.Fatal(err)
		}
		if !rx.ReceiveNext() {
			b.Fatal("warm-up receive failed")
		}
	}
	return tx, rx
}

func benchmarkBroadcastWarmTransmit(b *testing.B) *broadcast.BroadcastTransmitter {
	b.Helper()
	tx, _, _ := newTestBroadcastPair(b)
	const warm = 1024
	for range warm {
		if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
			b.Fatal(err)
		}
	}
	return tx
}

func BenchmarkBroadcastTransmitter_Transmit(b *testing.B) {
	tx := benchmarkBroadcastWarmTransmit(b)
	b.ResetTimer()
	for range b.N {
		if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBroadcastTransmitter_TransmitLapped(b *testing.B) {
	tx, _, _ := newTestBroadcastPair(b)
	for range writesToLap(testCapacity, len(testPayload8), 8) {
		if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for range b.N {
		if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBroadcastReceiver_ReceiveNext(b *testing.B) {
	tx, rx := benchmarkBroadcastWarm(b)
	b.ResetTimer()
	for range b.N {
		if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
			b.Fatal(err)
		}
		if !rx.ReceiveNext() {
			b.Fatal("ReceiveNext=false")
		}
	}
}

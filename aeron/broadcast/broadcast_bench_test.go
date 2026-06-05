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

func BenchmarkBroadcastTransmitter_Transmit(b *testing.B) {
	tx, _, _ := newTestBroadcastPair(b)
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

func BenchmarkBroadcastReceiver_Validate(b *testing.B) {
	_, rx := benchmarkBroadcastWarm(b)
	b.ResetTimer()
	for range b.N {
		_ = rx.Validate()
	}
}

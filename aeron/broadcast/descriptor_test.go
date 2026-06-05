package broadcast_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/broadcast"
)

func TestBroadcastDescriptorConstants(t *testing.T) {
	if broadcast.TrailerLength != 128 {
		t.Fatalf("TrailerLength=%d want 128", broadcast.TrailerLength)
	}
	if broadcast.MinDataCapacity != 16 {
		t.Fatalf("MinDataCapacity=%d want 16", broadcast.MinDataCapacity)
	}
	if broadcast.TailIntentOffset != 0 {
		t.Fatalf("TailIntentOffset=%d want 0", broadcast.TailIntentOffset)
	}
	if broadcast.TailOffset != 8 {
		t.Fatalf("TailOffset=%d want 8", broadcast.TailOffset)
	}
	if broadcast.LatestOffset != 16 {
		t.Fatalf("LatestOffset=%d want 16", broadcast.LatestOffset)
	}
}

func TestBroadcastDescriptorIndices(t *testing.T) {
	if got := broadcast.TailIntentIndex(testCapacity); got != testCapacity {
		t.Fatalf("TailIntentIndex=%d want %d", got, testCapacity)
	}
	if got := broadcast.TailIndex(testCapacity); got != testCapacity+8 {
		t.Fatalf("TailIndex=%d want %d", got, testCapacity+8)
	}
	if got := broadcast.LatestIndex(testCapacity); got != testCapacity+16 {
		t.Fatalf("LatestIndex=%d want %d", got, testCapacity+16)
	}
}

func TestBroadcastTotalBufferLength(t *testing.T) {
	if got := broadcast.TotalBufferLength(testCapacity); got != testCapacity+broadcast.TrailerLength {
		t.Fatalf("TotalBufferLength=%d want %d", got, testCapacity+broadcast.TrailerLength)
	}
}

func TestBroadcastCheckCapacity(t *testing.T) {
	data, err := broadcast.CheckCapacity(broadcast.TotalBufferLength(testCapacity))
	if err != nil {
		t.Fatal(err)
	}
	if data != testCapacity {
		t.Fatalf("dataCapacity=%d want %d", data, testCapacity)
	}
	totalLen := broadcast.TotalBufferLength(invalidDataCapacity)
	if _, err := broadcast.CheckCapacity(totalLen); err == nil {
		t.Fatal("expected error for non-power-of-two data capacity")
	}
	if _, err := broadcast.CheckCapacity(broadcast.TrailerLength + testCapacity - 1); err == nil {
		t.Fatal("expected error when buffer is too small for power-of-two data capacity")
	}
	if _, err := broadcast.CheckCapacity(broadcast.TotalBufferLength(8)); err == nil {
		t.Fatal("expected error for data capacity below minimum")
	}
}

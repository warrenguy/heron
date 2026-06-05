package ringbuffer_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

func TestRingBufferDescriptorConstants(t *testing.T) {
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"TrailerLength", ringbuffer.TrailerLength, 768},
		{"TailOffset", ringbuffer.TailOffset, 128},
		{"HeadCacheOffset", ringbuffer.HeadCacheOffset, 256},
		{"HeadOffset", ringbuffer.HeadOffset, 384},
		{"CorrelationOffset", ringbuffer.CorrelationOffset, 512},
		{"ConsumerHeartbeatOffset", ringbuffer.ConsumerHeartbeatOffset, 640},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Fatalf("%s=%d want %d", tc.name, tc.got, tc.want)
		}
	}
}

func TestRingBufferDescriptorIndices(t *testing.T) {
	const capacity = 1024
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"TailIndex", ringbuffer.TailIndex(capacity), capacity + 128},
		{"HeadCacheIndex", ringbuffer.HeadCacheIndex(capacity), capacity + 256},
		{"HeadIndex", ringbuffer.HeadIndex(capacity), capacity + 384},
		{"CorrelationIndex", ringbuffer.CorrelationIndex(capacity), capacity + 512},
		{"ConsumerHeartbeatIndex", ringbuffer.ConsumerHeartbeatIndex(capacity), capacity + 640},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Fatalf("%s=%d want %d", tc.name, tc.got, tc.want)
		}
	}
}

func TestTotalBufferLength(t *testing.T) {
	for _, dataCapacity := range []int{1024, ringbuffer.MinCapacity} {
		want := dataCapacity + ringbuffer.TrailerLength
		if got := ringbuffer.TotalBufferLength(dataCapacity); got != want {
			t.Fatalf("TotalBufferLength(%d)=%d want %d", dataCapacity, got, want)
		}
	}
}

func TestCheckCapacity(t *testing.T) {
	const minData = ringbuffer.MinCapacity
	total := ringbuffer.TotalBufferLength(minData)
	data, err := ringbuffer.CheckCapacity(total, minData)
	if err != nil {
		t.Fatal(err)
	}
	if data != minData {
		t.Fatalf("data capacity %d want %d", data, minData)
	}

	if _, err := ringbuffer.CheckCapacity(ringbuffer.TrailerLength+minData-1, minData); err == nil {
		t.Fatal("expected error for non-power-of-two data capacity")
	}

	if _, err := ringbuffer.CheckCapacity(ringbuffer.TotalBufferLength(minData-1), minData); err == nil {
		t.Fatal("expected error for insufficient capacity")
	}
}

func TestMinCapacityPowerOfTwo(t *testing.T) {
	if !atomic.IsPowerOfTwo(ringbuffer.MinCapacity) {
		t.Fatalf("MinCapacity %d not power of two", ringbuffer.MinCapacity)
	}
}

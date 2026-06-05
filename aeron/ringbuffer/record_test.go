package ringbuffer_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/ringbuffer"
)

func TestMaxMsgLength(t *testing.T) {
	cases := []struct {
		capacity int
		want     int
	}{
		{ringbuffer.MinCapacity, 0},
		{4096, 512},
	}
	for _, tc := range cases {
		if got := ringbuffer.MaxMsgLength(tc.capacity); got != tc.want {
			t.Fatalf("MaxMsgLength(%d)=%d want %d", tc.capacity, got, tc.want)
		}
	}
}

package broadcast_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/broadcast"
)

func TestBroadcastMaxMsgLength(t *testing.T) {
	cases := []struct {
		capacity int
		want     int
	}{
		{testCapacity, 128},
		{broadcast.MinDataCapacity, 2},
		{8, 0},
	}
	for _, tc := range cases {
		if got := broadcast.MaxMsgLength(tc.capacity); got != tc.want {
			t.Fatalf("MaxMsgLength(%d)=%d want %d", tc.capacity, got, tc.want)
		}
	}
}

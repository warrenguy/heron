package broadcast_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/broadcast"
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
		require.Equal(t, tc.want, broadcast.MaxMsgLength(tc.capacity))
	}
}

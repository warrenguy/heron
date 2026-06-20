package ringbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/agrona/ringbuffer"
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
		require.Equal(t, tc.want, ringbuffer.MaxMsgLength(tc.capacity))
	}
}

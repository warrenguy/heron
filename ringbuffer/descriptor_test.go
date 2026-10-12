package ringbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/ringbuffer"
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
		require.Equal(t, tc.want, tc.got, tc.name)
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
		require.Equal(t, tc.want, tc.got, tc.name)
	}
}

func TestTotalBufferLength(t *testing.T) {
	for _, dataCapacity := range []int{1024, ringbuffer.MinCapacity} {
		want := dataCapacity + ringbuffer.TrailerLength
		require.Equal(t, want, ringbuffer.TotalBufferLength(dataCapacity))
	}
}

func TestCheckCapacity(t *testing.T) {
	const minData = ringbuffer.MinCapacity
	total := ringbuffer.TotalBufferLength(minData)
	data, err := ringbuffer.CheckCapacity(total, minData)
	require.NoError(t, err)
	require.Equal(t, minData, data)

	_, err = ringbuffer.CheckCapacity(ringbuffer.TrailerLength+minData-1, minData)
	require.Error(t, err)

	tooSmall := ringbuffer.TotalBufferLength(minData - 1)
	_, err = ringbuffer.CheckCapacity(tooSmall, minData)
	require.Error(t, err)

	tooLarge := ringbuffer.TotalBufferLength(2 * atomicbuffer.MaxDataCapacity)
	_, err = ringbuffer.CheckCapacity(tooLarge, minData)
	require.EqualError(t, err, "ringbuffer: data capacity above maximum")
}

func TestMinCapacityPowerOfTwo(t *testing.T) {
	require.True(t, atomicbuffer.IsPowerOfTwo(ringbuffer.MinCapacity))
}

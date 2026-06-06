package broadcast_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/broadcast"
)

func TestBroadcastDescriptorConstants(t *testing.T) {
	require.Equal(t, 128, broadcast.TrailerLength)
	require.Equal(t, 16, broadcast.MinDataCapacity)
	require.Equal(t, 0, broadcast.TailIntentOffset)
	require.Equal(t, 8, broadcast.TailOffset)
	require.Equal(t, 16, broadcast.LatestOffset)
}

func TestBroadcastDescriptorIndices(t *testing.T) {
	require.Equal(t, testCapacity, broadcast.TailIntentIndex(testCapacity))
	require.Equal(t, testCapacity+8, broadcast.TailIndex(testCapacity))
	require.Equal(t, testCapacity+16, broadcast.LatestIndex(testCapacity))
}

func TestBroadcastTotalBufferLength(t *testing.T) {
	require.Equal(t, testCapacity+broadcast.TrailerLength, broadcast.TotalBufferLength(testCapacity))
}

func TestBroadcastCheckCapacity(t *testing.T) {
	data, err := broadcast.CheckCapacity(broadcast.TotalBufferLength(testCapacity))
	require.NoError(t, err)
	require.Equal(t, testCapacity, data)

	totalLen := broadcast.TotalBufferLength(invalidDataCapacity)
	_, err = broadcast.CheckCapacity(totalLen)
	require.Error(t, err)

	_, err = broadcast.CheckCapacity(broadcast.TrailerLength + testCapacity - 1)
	require.Error(t, err)

	_, err = broadcast.CheckCapacity(broadcast.TotalBufferLength(8))
	require.Error(t, err)
}

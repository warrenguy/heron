package atomicbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
)

func TestDataCapacity(t *testing.T) {
	const trailer, min = 128, 16
	got, err := atomicbuffer.DataCapacity(trailer+1024, trailer, min)
	require.NoError(t, err)
	require.Equal(t, 1024, got)
}

func TestDataCapacity_NotPowerOfTwo(t *testing.T) {
	_, err := atomicbuffer.DataCapacity(128+100, 128, 16)
	require.ErrorIs(t, err, atomicbuffer.ErrDataCapacityNotPowerOfTwo)
}

func TestDataCapacity_BelowMinimum(t *testing.T) {
	_, err := atomicbuffer.DataCapacity(128+8, 128, 16)
	require.ErrorIs(t, err, atomicbuffer.ErrDataCapacityBelowMinimum)
}

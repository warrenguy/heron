package record_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/record"
)

func TestRecordLayoutDerivedFromAtomicSizes(t *testing.T) {
	require.Equal(t, atomic.SizeInt32, record.TypeFieldOffset)
	require.Equal(t, atomic.SizeInt32+atomic.SizeInt32, record.HeaderLength)
	require.Equal(t, record.TypeFieldOffset, record.TypeOffset(0)-record.LengthOffset(0))
}

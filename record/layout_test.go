package record_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/record"
)

func TestRecordLayoutDerivedFromAtomicSizes(t *testing.T) {
	require.Equal(t, atomicbuffer.SizeInt32, record.TypeFieldOffset)
	require.Equal(t, atomicbuffer.SizeInt32+atomicbuffer.SizeInt32, record.HeaderLength)
	require.Equal(t, record.TypeFieldOffset, record.TypeOffset(0)-record.LengthOffset(0))
}

//go:build rapid

package atomicbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/atomicbuffer"
)

func drawOutOfBoundsOffset(rt *rapid.T, capacity int) int {
	rt.Helper()
	return rapid.IntRange(-64, capacity+64).
		Filter(func(offset int) bool { return offset < 0 || offset >= capacity }).
		Draw(rt, "offset")
}

func TestGetByte_outOfBounds_panics_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(0, 128).Draw(rt, "capacity")
		offset := drawOutOfBoundsOffset(rt, capacity)
		buf := atomicbuffer.Wrap(make([]byte, capacity))
		require.Panics(rt, func() { buf.GetByte(offset) })
	})
}

func TestPutByte_outOfBounds_panics_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(0, 128).Draw(rt, "capacity")
		offset := drawOutOfBoundsOffset(rt, capacity)
		value := rapid.Byte().Draw(rt, "value")
		buf := atomicbuffer.Wrap(make([]byte, capacity))
		require.Panics(rt, func() { buf.PutByte(offset, value) })
	})
}

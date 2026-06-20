//go:build rapid

package atomic_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/agrona/atomic"
)

func TestAlign_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		value := rapid.IntRange(0, 1<<18).Draw(t, "value")
		shift := rapid.IntRange(0, 10).Draw(t, "shift")
		align := 1 << shift
		got := atomic.Align(value, align)
		require.Equal(t, 0, got%align, "Align(%d,%d)=%d not aligned", value, align, got)
		require.GreaterOrEqual(t, got, value, "Align(%d,%d)=%d < value", value, align, got)
		require.Less(t, got-value, align, "Align(%d,%d)=%d overshot", value, align, got)
	})
}

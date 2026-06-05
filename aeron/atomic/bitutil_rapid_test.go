package atomic_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"pgregory.net/rapid"
)

func TestAlign_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		value := rapid.IntRange(0, 1<<18).Draw(t, "value")
		shift := rapid.IntRange(0, 10).Draw(t, "shift")
		align := 1 << shift
		got := atomic.Align(value, align)
		if got%align != 0 {
			t.Fatalf("Align(%d,%d)=%d not aligned", value, align, got)
		}
		if got < value {
			t.Fatalf("Align(%d,%d)=%d < value", value, align, got)
		}
		if got-value >= align {
			t.Fatalf("Align(%d,%d)=%d overshot", value, align, got)
		}
	})
}

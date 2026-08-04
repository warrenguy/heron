package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCaptureFailTB_recordsFailAndSkipWithoutStoppingParent asserts Fail and
// Skip paths set capture state and leave the parent running.
func TestCaptureFailTB_recordsFailAndSkipWithoutStoppingParent(t *testing.T) {
	t.Run("Fatalf marks failed", func(t *testing.T) {
		capture := NewCaptureFailTB(t)
		capture.Fatalf("fail %s", "now")
		require.True(t, capture.Failed())
		require.False(t, capture.Skipped())
		require.Equal(t, "fail now", capture.lastMsg)
	})

	t.Run("Skip marks skipped not failed", func(t *testing.T) {
		capture := NewCaptureFailTB(t)
		capture.Skip("missing runtime")
		require.False(t, capture.Failed())
		require.True(t, capture.Skipped())
		require.Equal(t, "missing runtime", capture.lastMsg)
	})

	t.Run("Skipf marks skipped not failed", func(t *testing.T) {
		capture := NewCaptureFailTB(t)
		capture.Skipf("missing %s", "runtime")
		require.False(t, capture.Failed())
		require.True(t, capture.Skipped())
		require.Equal(t, "missing runtime", capture.lastMsg)
	})

	t.Run("SkipNow marks skipped not failed", func(t *testing.T) {
		capture := NewCaptureFailTB(t)
		capture.SkipNow()
		require.False(t, capture.Failed())
		require.True(t, capture.Skipped())
	})
}

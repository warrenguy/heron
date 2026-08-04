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

	t.Run("multi-line Error body stays searchable", func(t *testing.T) {
		capture := NewCaptureFailTB(t)
		// Shape matches testify Fail: Error Trace header, multi-line Error block.
		capture.Errorf("%s", ""+
			"\nError Trace:\t/tmp/x.go:1"+
			"\nError:      \tReceived unexpected error:"+
			"\n            \tmember 0: inject member 0"+
			"\n            \tmember 1: inject member 1"+
			"\nTest:       \tTestFoo\n")
		require.Contains(t, capture.lastMsg, "member 0: inject member 0")
		require.Contains(t, capture.lastMsg, "member 1: inject member 1")
		require.Contains(t, capture.lastMsg, "Received unexpected error:")
	})
}

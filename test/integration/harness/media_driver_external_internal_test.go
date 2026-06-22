//go:build integration

package harness

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinExternal_cncNotReady(t *testing.T) {
	t.Parallel()
	capture := &CaptureFailTB{TB: t}
	JoinExternalTimeout(capture, t.TempDir(), pollHarnessShortTimeout)
	require.True(t, capture.failed)
	require.Contains(t, capture.lastMsg, "not ready")
}

func TestAssertCncAlive_missingCnc(t *testing.T) {
	t.Parallel()
	capture := &CaptureFailTB{TB: t}
	AssertCncAlive(capture, t.TempDir())
	require.True(t, capture.failed)
}

func TestJoinExternal_stopPreservesDir(t *testing.T) {
	t.Parallel()
	started := Start(t)

	joined := JoinExternal(t, started.Dir)
	joined.Stop()

	_, err := os.Stat(started.CncPath())
	require.NoError(t, err)
}

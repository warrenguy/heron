//go:build integration

package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRequireArchiveRuntime asserts RequireArchiveRuntime fails the test when
// the Archive jar or java is absent and passes when both are present.
func TestRequireArchiveRuntime(t *testing.T) {
	t.Run("runtime present", func(t *testing.T) {
		if _, err := exec.LookPath("java"); err != nil {
			t.Skip("java not on PATH; cannot exercise present-runtime path")
		}
		if _, err := resolveArchiveJar(); err != nil {
			t.Skip("archive jar not resolved; cannot exercise present-runtime path")
		}

		RequireArchiveRuntime(t)
	})

	t.Run("jar missing", func(t *testing.T) {
		t.Setenv(envArchiveJar, filepath.Join(t.TempDir(), "absent.jar"))

		capture := NewCaptureFailTB(t)
		RequireArchiveRuntime(capture)
		requireRuntimeFail(t, capture)
	})

	t.Run("java missing", func(t *testing.T) {
		jar := filepath.Join(t.TempDir(), "present.jar")
		require.NoError(t, os.WriteFile(jar, []byte("jar"), 0o600))
		t.Setenv(envArchiveJar, jar)
		t.Setenv("PATH", t.TempDir())

		capture := NewCaptureFailTB(t)
		RequireArchiveRuntime(capture)
		requireRuntimeFail(t, capture)
	})
}

func requireRuntimeFail(t *testing.T, capture *CaptureFailTB) {
	t.Helper()
	require.True(t, capture.Failed())
	require.False(t, capture.Skipped())
	require.Contains(t, capture.lastMsg, archiveRuntimeMissing)
}

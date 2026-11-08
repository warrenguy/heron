//go:build rapid

package testutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestIterationTB_cleanupRunsPerCheck_rapid(t *testing.T) {
	var registered, ran int
	var lastDir string
	rapid.Check(t, func(rt *rapid.T) {
		require.Equal(rt, registered, ran)

		if lastDir != "" {
			require.NoDirExists(rt, lastDir)
		}

		tb := IterationTB(t, rt)
		registered++
		tb.Cleanup(func() { ran++ })
		lastDir = tb.TempDir()
		require.DirExists(rt, lastDir)
		require.NoError(rt, os.WriteFile(filepath.Join(lastDir, "f"), []byte("x"), 0o600))
	})

	require.Positive(t, registered)
	require.Equal(t, registered, ran)
	require.NoDirExists(t, lastDir)
}

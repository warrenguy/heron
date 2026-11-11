//go:build testhooks

package scripts_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheFetchIfMissing(t *testing.T) {
	lib := filepath.Join(repoRoot(t), "scripts", "lib", "cache-fetch.sh")
	payload := []byte("sbe-tool-bytes\n")
	sum := sha256.Sum256(payload)
	match := hex.EncodeToString(sum[:])
	wrong := strings.Repeat("ab", 32)

	tests := []struct {
		name   string
		cached bool
		sha    string
		wantOK bool
	}{
		{name: "fresh matching hash", sha: match, wantOK: true},
		{name: "fresh wrong hash", sha: wrong},
		{name: "cached matching hash", cached: true, sha: match, wantOK: true},
		{name: "cached wrong hash", cached: true, sha: wrong},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dest := filepath.Join(dir, "tool.jar")
			src := filepath.Join(dir, "src.jar")
			require.NoError(t, os.WriteFile(src, payload, 0o644))

			if tc.cached {
				require.NoError(t, os.WriteFile(dest, payload, 0o644))
			}

			srcURL := (&url.URL{Scheme: "file", Path: src}).String()
			cmd := exec.Command("bash", "-c", `source "$1" && cache_fetch_if_missing "$2" "$3" "$4"`,
				"bash", lib, dest, srcURL, tc.sha)
			out, err := cmd.CombinedOutput()

			if tc.wantOK {
				require.NoError(t, err, "output:\n%s", out)
			} else {
				require.Error(t, err)
				require.Contains(t, string(out), "sha256 mismatch")
			}

			_, statErr := os.Stat(dest)

			if tc.wantOK || tc.cached {
				require.NoError(t, statErr)
			} else {
				require.ErrorIs(t, statErr, fs.ErrNotExist)
			}
		})
	}
}

func TestRequireVersions(t *testing.T) {
	root := repoRoot(t)
	script := `source "$1" && require_aeron_versions "$2" && require_sbe_versions "$2"`
	cmd := exec.Command("bash", "-c", script, "bash",
		filepath.Join(root, "scripts", "lib", "versions.sh"),
		filepath.Join(root, "specs", "versions.yaml"))
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "output:\n%s", out)
}

func repoRoot(tb testing.TB) string {
	tb.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

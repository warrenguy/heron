//go:build integration

package harness

import (
	"errors"
	"os/exec"
	"testing"
)

// RequireArchiveRuntime fails the test when the Archive jar or a Java runtime is
// absent.
//
// Behaviour: resolves the Archive jar the same way StartArchiveNode does, then
// looks up java on PATH. Fails the test when either is missing. Call this from
// a test that must exercise live Archive behaviour. StartArchiveNode Skips on
// a missing jar or java, so a live-path test can pass without running Archive.
//
// Concurrency: invoker-only.
//
// Reports a missing runtime through testing.TB, not an error return. No caller
// can proceed without the runtime.
func RequireArchiveRuntime(t testing.TB) {
	t.Helper()

	if _, err := resolveArchiveJar(); err != nil {
		if errors.Is(err, errArchiveJarNotFound) {
			t.Fatalf("%s", archiveRuntimeMissing)
		}
		t.Fatalf("%s: %v", archiveRuntimeMissing, err)
	}
	if _, err := exec.LookPath("java"); err != nil {
		t.Fatalf("%s", archiveRuntimeMissing)
	}
}

//go:build testhooks

package cluster

import (
	"testing"

	"github.com/warrenguy/heron/internal/testhook"
)

// directoryLockBlockedHooks keys hooks by cluster.lock path. Concurrent
// OpenRecovery peers must not observe another test's hook.
var directoryLockBlockedHooks = testhook.NewRegistry[string, func()]()

func trackDirectoryLockBlocked(path string) {
	if hook := directoryLockBlockedHooks.Load(path); hook != nil {
		hook()
	}
}

// SetDirectoryLockBlockedHookForTest installs a hook that runs on each
// EWOULDBLOCK while [OpenRecovery] acquires the lock at path.
//
// path must be the cluster.lock path under the cluster directory OpenRecovery
// will lock. Callers may release a trailing holder in the hook so the next
// attempt can succeed inside [DirectoryLockWait].
//
// Lifetime: the hook stays set for the remainder of tb, including subtests.
func SetDirectoryLockBlockedHookForTest(tb testing.TB, path string, hook func()) {
	tb.Helper()
	testhook.SetForTest(tb, directoryLockBlockedHooks, path, hook)
}

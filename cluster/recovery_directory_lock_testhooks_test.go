//go:build testhooks

package cluster_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/cluster"
)

// TestOpenRecovery_lockWaitsForTrailingRelease asserts a second OpenRecovery
// succeeds when a foreign holder releases cluster.lock during the open wait.
//
// The fixture holds LOCK_EX and releases with Close only. The fixture does not
// call LOCK_UN. The exclusive hold ends when the last open-file-description
// reference goes away. That path is not [Recovery.Close]. [Recovery.Close]
// unlocks with LOCK_UN through [releaseClusterLock] before Close.
//
// OpenRecovery must not refuse on the first EWOULDBLOCK. On the first blocked
// attempt the directory-lock blocked hook closes the holder so the open can
// finish inside [DirectoryLockWait].
func TestOpenRecovery_lockWaitsForTrailingRelease(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "cluster.lock")
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	require.NoError(t, err)
	require.NoError(t, unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB))

	// holder wraps the flock descriptor. Close is close-once. The release below
	// and this cleanup share one close path.
	holder := os.NewFile(uintptr(fd), path)
	t.Cleanup(func() { _ = holder.Close() })

	cfg := recoveryConfig(t, dir)

	// openFDCount and fdsUnder Skip when GOOS is not linux. Probe before any
	// OpenRecovery goroutine starts so a Skip cannot leave a waiter running.
	baseline := openFDCount(t, path)
	require.Equal(t, 1, baseline, "holder must own one open cluster.lock fd")

	type openResult struct {
		recovery *cluster.Recovery
		err      error
	}
	// First EWOULDBLOCK signals OpenRecovery is retrying. Release the holder
	// then. DirectoryLockWait does not race a readiness poll. Install the hook
	// before the goroutine starts so the first attempt can observe it.
	blocked := make(chan struct{})
	cluster.SetDirectoryLockBlockedHookForTest(t, path, sync.OnceFunc(func() {
		close(blocked)
		_ = holder.Close()
	}))
	done := make(chan openResult, 1)
	go func() {
		recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{Config: cfg})
		done <- openResult{recovery, err}
	}()

	// Cleanup owns the OpenRecovery waiter on every exit. Closes holder first so
	// flock can complete, then drains done unless gotResult is already set. Closes
	// any Recovery from the open result.
	var (
		result    openResult
		gotResult bool
	)
	t.Cleanup(func() {
		_ = holder.Close()
		if !gotResult {
			select {
			case result = <-done:
				gotResult = true
			case <-time.After(cluster.DirectoryLockWait + time.Second):
				return
			}
		}
		if result.recovery != nil {
			_ = result.recovery.Close()
		}
	})

	select {
	case result = <-done:
		gotResult = true
	case <-time.After(cluster.DirectoryLockWait + time.Second):
		t.Fatal("OpenRecovery did not return after holder release")
	}

	// OpenRecovery sends on done after the release callback. blocked is already
	// closed here. A dual select on blocked and done would choose either case at
	// random once OpenRecovery has finished.
	select {
	case <-blocked:
	default:
		t.Fatal("OpenRecovery finished without observing EWOULDBLOCK")
	}

	require.NoError(t, result.err)
	require.NotNil(t, result.recovery)
}

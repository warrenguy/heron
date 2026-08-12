//go:build integration

package harness

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	// sharedDriverStateMu guards sharedDriver, testMainSharedDriver, and
	// startSharedDriverHook. Self-tests take the write lock for each rewrite.
	// Shared samples under the read lock so SharedParallel peers never race a
	// mid-suite rewrite.
	sharedDriverStateMu  sync.RWMutex
	sharedDriver         *MediaDriver
	sharedTestMainSeen   atomic.Bool
	testMainSharedDriver *MediaDriver

	startSharedDriverHook func() (*MediaDriver, error)

	// forceSharedMediaDriverDisabled and forceSharedMediaDriverKeep inject
	// disable and keep without os.Setenv. SharedParallel peers that getenv the
	// process env must not race self-test Setenv spans.
	forceSharedMediaDriverDisabled atomic.Bool
	forceSharedMediaDriverKeep     atomic.Bool
)

type testMainRunner interface {
	Run() int
}

func sharedMediaDriverDisabled() bool {
	return forceSharedMediaDriverDisabled.Load() ||
		os.Getenv("HERON_SHARED_MEDIA_DRIVER") == "0"
}

func sharedMediaDriverKeep() bool {
	return forceSharedMediaDriverKeep.Load() ||
		os.Getenv("HERON_SHARED_MEDIA_DRIVER_KEEP") == "1"
}

// forceSharedDriverDisabledForTest makes sharedMediaDriverDisabled report true
// until tb finishes.
func forceSharedDriverDisabledForTest(tb testing.TB) {
	tb.Helper()
	forceSharedMediaDriverDisabled.Store(true)
	tb.Cleanup(func() { forceSharedMediaDriverDisabled.Store(false) })
}

// forceSharedDriverKeepForTest makes sharedMediaDriverKeep report true until tb
// finishes.
func forceSharedDriverKeepForTest(tb testing.TB) {
	tb.Helper()
	forceSharedMediaDriverKeep.Store(true)
	tb.Cleanup(func() { forceSharedMediaDriverKeep.Store(false) })
}

func startSharedDriver() (*MediaDriver, error) {
	sharedDriverStateMu.RLock()
	hook := startSharedDriverHook
	sharedDriverStateMu.RUnlock()
	if hook != nil {
		return hook()
	}

	aeronmdPath, err := resolveAeronmd()
	if err != nil {
		return nil, err
	}
	return startDriverInTempDir(aeronmdPath, "heron-aeron-shared-*", nil)
}

func stopSharedDriver(md *MediaDriver) {
	if md == nil {
		return
	}
	if sharedMediaDriverKeep() {
		fmt.Fprintf(
			os.Stderr,
			"harness: keeping shared media driver dir=%s (HERON_SHARED_MEDIA_DRIVER_KEEP=1)\n",
			md.Dir,
		)
		return
	}
	md.Stop()
}

// RunTestMainWithSharedDriver starts the shared driver before m.Run and stops it after.
// Call from each package's TestMain. Returns exit code for os.Exit.
//
// HERON_SHARED_MEDIA_DRIVER and HERON_SHARED_MEDIA_DRIVER_KEEP are documented in the
// package comment on media_driver.go.
//
// When aeronmd is not found, logs to stderr and returns m.Run() without starting a shared
// driver. All other start failures (CNC not ready, exec error, env validation, bad
// AERONMD_PATH) log to stderr and return 1 without calling m.Run().
func RunTestMainWithSharedDriver(m *testing.M) int {
	return runTestMainWithSharedDriver(m)
}

func runTestMainWithSharedDriver(m testMainRunner) int {
	if sharedMediaDriverDisabled() {
		return m.Run()
	}

	md, err := startSharedDriver()
	if err != nil {
		if errors.Is(err, errAeronmdNotFound) {
			fmt.Fprintf(
				os.Stderr,
				"harness: aeronmd not found; run make test-integration (requires Docker)\n",
			)
			return m.Run()
		}
		fmt.Fprintf(os.Stderr, "harness: shared media driver start failed: %v\n", err)
		return 1
	}

	sharedDriverStateMu.Lock()
	snapshottedForTestMain := testMainSharedDriver == nil
	sharedTestMainSeen.Store(true)
	sharedDriver = md
	if snapshottedForTestMain {
		testMainSharedDriver = md
	}
	sharedDriverStateMu.Unlock()
	fmt.Fprintf(os.Stderr, "harness: shared media driver started dir=%s pid=%d\n", md.Dir, md.pid)

	start := time.Now()
	code := m.Run()
	dir := md.Dir
	elapsed := time.Since(start).Round(time.Millisecond)
	stopSharedDriver(md)
	if sharedMediaDriverKeep() {
		fmt.Fprintf(
			os.Stderr,
			"harness: shared media driver test run finished dir=%s elapsed=%s exit=%d\n",
			dir,
			elapsed,
			code,
		)
	} else {
		fmt.Fprintf(
			os.Stderr,
			"harness: shared media driver stopped dir=%s elapsed=%s exit=%d\n",
			dir,
			elapsed,
			code,
		)
	}
	sharedDriverStateMu.Lock()
	sharedDriver = nil
	if snapshottedForTestMain {
		testMainSharedDriver = nil
		sharedTestMainSeen.Store(false)
	}
	sharedDriverStateMu.Unlock()
	return code
}

func restoreTestMainSharedDriverGlobals() {
	sharedDriverStateMu.Lock()
	defer sharedDriverStateMu.Unlock()
	if testMainSharedDriver == nil {
		return
	}
	sharedDriver = testMainSharedDriver
	sharedTestMainSeen.Store(true)
}

// Shared returns the package-scoped long-lived driver when TestMain started it.
// When HERON_SHARED_MEDIA_DRIVER=0, falls back to Start(t) for per-test ephemeral driver.
func Shared(t testing.TB) *MediaDriver {
	t.Helper()
	if sharedMediaDriverDisabled() || !sharedTestMainSeen.Load() {
		return Start(t)
	}

	// sharedDriverStateMu guards sharedDriver only. The checks above read
	// atomics and the process env.
	sharedDriverStateMu.RLock()
	md := sharedDriver
	sharedDriverStateMu.RUnlock()
	if md == nil {
		t.Fatal("harness: shared media driver not started")
	}
	md.AssertAlive(t)
	return md
}

// SharedParallel calls t.Parallel then returns the package-scoped shared driver.
// Do not call t.Parallel again in the same test or subtest; Go panics on a second call.
func SharedParallel(t *testing.T) *MediaDriver {
	t.Helper()
	t.Parallel()
	return Shared(t)
}

// SharedAttachedCnc returns Shared(t) with CNC mmap attach and ToDriver/ToClients drain preflight.
func SharedAttachedCnc(t testing.TB) (*MediaDriver, *cncAttach) {
	t.Helper()
	md := Shared(t)
	attach := openCnc(t, md)
	WaitForToDriverRingDrained(t, attach.Rings.ToDriver, 0)
	DrainToClients(t, attach.Rings.ToClients, 0)
	return md, attach
}

// AssertSharedDriverWiring proves the calling package's TestMain leaves Shared(t)
// returning one stable package driver across subtests.
//
// Uses public Shared(t) only; ephemeral Start(t) fallback returns distinct instances,
// so require.Same still catches missing TestMain wiring.
func AssertSharedDriverWiring(t *testing.T) {
	t.Helper()
	if sharedMediaDriverDisabled() {
		t.Skip("HERON_SHARED_MEDIA_DRIVER=0")
	}

	var shared *MediaDriver
	t.Run("first", func(t *testing.T) {
		shared = Shared(t)
		shared.AssertAlive(t)
	})
	t.Run("second", func(t *testing.T) {
		md := Shared(t)
		require.Same(t, shared, md)
		md.AssertAlive(t)
	})
}

// AssertSharedDriverFallback proves Shared(t) falls back to ephemeral Start(t) when
// Shared mode is disabled so migrated call sites still work with shared off.
//
// Uses a process-local force flag rather than os.Setenv. SharedParallel peers
// that read HERON_SHARED_MEDIA_DRIVER must not race this test.
func AssertSharedDriverFallback(t *testing.T) {
	t.Helper()
	if sharedMediaDriverDisabled() {
		t.Skip("HERON_SHARED_MEDIA_DRIVER=0")
	}

	shared := Shared(t)
	shared.AssertAlive(t)

	forceSharedDriverDisabledForTest(t)

	ephemeral := Shared(t)
	require.NotSame(t, shared, ephemeral)
	require.NotEqual(t, shared.Dir, ephemeral.Dir)
	ephemeral.AssertAlive(t)
}

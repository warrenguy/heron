//go:build integration

package harness

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

type stubTestMain struct {
	runCode int
	ran     bool
}

func (s *stubTestMain) Run() int {
	s.ran = true
	return s.runCode
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	orig := os.Stderr
	os.Stderr = w
	defer func() {
		os.Stderr = orig
	}()

	fn()

	require.NoError(t, w.Close())

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return buf.String()
}

func resetSharedDriverGlobals(t *testing.T) {
	t.Helper()

	// Self-tests must stay serial: write lock around each rewrite so
	// SharedParallel peers sample a consistent snapshot under RLock.
	sharedDriverStateMu.Lock()
	sharedDriver = nil
	sharedTestMainSeen.Store(false)
	startSharedDriverHook = nil
	sharedDriverStateMu.Unlock()
	t.Cleanup(restoreTestMainSharedDriverGlobals)
}

func assertSharedDriverGlobalsUnset(t *testing.T) {
	t.Helper()

	sharedDriverStateMu.RLock()
	md := sharedDriver
	sharedDriverStateMu.RUnlock()
	require.Nil(t, md)
}

func requireSharedDriverOrSkip(t *testing.T) *MediaDriver {
	t.Helper()

	md, err := startSharedDriver()
	if errors.Is(err, errAeronmdNotFound) {
		t.Skip("aeronmd not found; run make test-integration (requires Docker)")
	}
	require.NoError(t, err)
	return md
}

// setSharedDriverHookForTest installs hook and clears hook when t finishes.
//
// Lifetime: the hook stays set for the remainder of t, including subtests.
func setSharedDriverHookForTest(t *testing.T, hook func() (*MediaDriver, error)) {
	t.Helper()

	sharedDriverStateMu.Lock()
	startSharedDriverHook = hook
	sharedDriverStateMu.Unlock()
	t.Cleanup(func() {
		sharedDriverStateMu.Lock()
		startSharedDriverHook = nil
		sharedDriverStateMu.Unlock()
	})
}

func installSharedDriverStubHook(t *testing.T) *MediaDriver {
	t.Helper()

	md := requireSharedDriverOrSkip(t)
	setSharedDriverHookForTest(t, func() (*MediaDriver, error) {
		return md, nil
	})
	return md
}

func primeSharedDriverForTest(t *testing.T) *MediaDriver {
	t.Helper()

	md := requireSharedDriverOrSkip(t)

	sharedDriverStateMu.Lock()
	sharedDriver = md
	sharedTestMainSeen.Store(true)
	sharedDriverStateMu.Unlock()
	t.Cleanup(func() {
		stopSharedDriver(md)
		resetSharedDriverGlobals(t)
	})
	return md
}

func TestSharedDriver_reusedAcrossSubtests(t *testing.T) {
	resetSharedDriverGlobals(t)

	md := primeSharedDriverForTest(t)

	var dir string
	t.Run("first", func(t *testing.T) {
		got := Shared(t)
		dir = got.Dir
		got.AssertAlive(t)
	})
	t.Run("second", func(t *testing.T) {
		got := Shared(t)
		require.Equal(t, dir, got.Dir)
		require.Same(t, md, got)
		got.AssertAlive(t)
	})
}

func TestShared_fallback_disabled(t *testing.T) {
	resetSharedDriverGlobals(t)

	sharedMD := primeSharedDriverForTest(t)
	forceSharedDriverDisabledForTest(t)

	ephemeral := Shared(t)
	require.NotSame(t, sharedMD, ephemeral)
	require.NotEqual(t, sharedMD.Dir, ephemeral.Dir)
	ephemeral.AssertAlive(t)
}

func TestShared_fallback_testMainNotSeen(t *testing.T) {
	resetSharedDriverGlobals(t)

	got := Shared(t)
	require.NotEmpty(t, got.Dir)
	got.AssertAlive(t)
}

func TestSharedAttachedCnc_wiresRingsAndDrains(t *testing.T) {
	resetSharedDriverGlobals(t)

	sharedMD := primeSharedDriverForTest(t)

	md, attach := SharedAttachedCnc(t)
	require.Same(t, sharedMD, md)
	require.NotNil(t, attach)
	assertRingConstruct(t, attach)
	assertMetadataMatchesMmap(t, md, attach)
}

func TestStopSharedDriver_keep(t *testing.T) {
	resetSharedDriverGlobals(t)

	md := requireSharedDriverOrSkip(t)
	forceSharedDriverKeepForTest(t)
	t.Cleanup(md.Stop)

	stderr := captureStderr(t, func() {
		stopSharedDriver(md)
	})
	require.Contains(t, stderr, "keeping shared media driver")
	require.Contains(t, stderr, md.Dir)

	md.AssertAlive(t)
	_, err := os.Stat(md.Dir)
	require.NoError(t, err)
}

func TestRunTestMainWithSharedDriver(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(t *testing.T) *stubTestMain
		wantCode       int
		wantRan        bool
		stderrContains []string
	}{
		{
			name: "disabled",
			setup: func(t *testing.T) *stubTestMain {
				forceSharedDriverDisabledForTest(t)
				return &stubTestMain{runCode: 7}
			},
			wantCode: 7,
			wantRan:  true,
		},
		{
			name: "aeronmd_not_found",
			setup: func(t *testing.T) *stubTestMain {
				setSharedDriverHookForTest(t, func() (*MediaDriver, error) {
					return nil, errAeronmdNotFound
				})
				return &stubTestMain{runCode: 3}
			},
			wantCode:       3,
			wantRan:        true,
			stderrContains: []string{"aeronmd not found"},
		},
		{
			name: "start_failure",
			setup: func(t *testing.T) *stubTestMain {
				setSharedDriverHookForTest(t, func() (*MediaDriver, error) {
					return nil, errors.New("start aeronmd: permission denied")
				})
				return &stubTestMain{runCode: 0}
			},
			wantCode:       1,
			wantRan:        false,
			stderrContains: []string{"shared media driver start failed"},
		},
		{
			name: "bad_aeronmd_path",
			setup: func(t *testing.T) *stubTestMain {
				t.Setenv("AERONMD_PATH", "/nonexistent/heron-aeronmd-missing")
				return &stubTestMain{runCode: 0}
			},
			wantCode:       1,
			wantRan:        false,
			stderrContains: []string{"shared media driver start failed"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetSharedDriverGlobals(t)

			stub := tc.setup(t)

			var code int
			if len(tc.stderrContains) > 0 {
				stderr := captureStderr(t, func() {
					code = runTestMainWithSharedDriver(stub)
				})
				for _, want := range tc.stderrContains {
					require.Contains(t, stderr, want)
				}
			} else {
				code = runTestMainWithSharedDriver(stub)
			}

			require.Equal(t, tc.wantCode, code)
			require.Equal(t, tc.wantRan, stub.ran)
			assertSharedDriverGlobalsUnset(t)
		})
	}
}

func TestRunTestMainWithSharedDriver_keep_logsAccurateLifecycle(t *testing.T) {
	resetSharedDriverGlobals(t)

	md := installSharedDriverStubHook(t)
	forceSharedDriverKeepForTest(t)
	t.Cleanup(func() {
		md.Stop()
		restoreTestMainSharedDriverGlobals()
	})

	stub := &stubTestMain{runCode: 42}
	stderr := captureStderr(t, func() {
		code := runTestMainWithSharedDriver(stub)
		require.Equal(t, 42, code)
	})
	require.True(t, stub.ran)
	require.Contains(t, stderr, "shared media driver started")
	require.Contains(t, stderr, "keeping shared media driver")
	require.Contains(t, stderr, "test run finished")
	require.Contains(t, stderr, fmt.Sprintf("dir=%s", md.Dir))
	require.Contains(t, stderr, "exit=42")
	require.NotContains(t, stderr, "shared media driver stopped")

	md.AssertAlive(t)
	_, err := os.Stat(md.Dir)
	require.NoError(t, err)
}

func TestRunTestMainWithSharedDriver_success_logsLifecycle(t *testing.T) {
	resetSharedDriverGlobals(t)

	md := installSharedDriverStubHook(t)
	t.Cleanup(restoreTestMainSharedDriverGlobals)

	stub := &stubTestMain{runCode: 42}
	stderr := captureStderr(t, func() {
		code := runTestMainWithSharedDriver(stub)
		require.Equal(t, 42, code)
	})
	require.True(t, stub.ran)
	require.Contains(t, stderr, "shared media driver started")
	require.Contains(t, stderr, fmt.Sprintf("dir=%s", md.Dir))
	require.Contains(t, stderr, "shared media driver stopped")
	require.Contains(t, stderr, "exit=42")
	assertSharedDriverGlobalsUnset(t)
}

func TestBuildDriverEnv(t *testing.T) {
	t.Parallel()
	const dir = "/tmp/heron-aeron-shared-test"
	const threadingOverride = "DEDICATED"

	tests := []struct {
		name              string
		overrides         DriverEnv
		wantIPCTermLength int
		wantThreadingMode string
		wantSyntaxErr     bool
		errContains       string
	}{
		{
			name:              "valid_defaults",
			wantIPCTermLength: logbuffer.TermMinLength,
			wantThreadingMode: defaultDriverEnv[envThreadingMode],
		},
		{
			name: "valid_override",
			overrides: DriverEnv{
				envIPCTermBufferLength: "131072",
			},
			wantIPCTermLength: 131072,
			wantThreadingMode: defaultDriverEnv[envThreadingMode],
		},
		{
			name: "override_wins",
			overrides: DriverEnv{
				envThreadingMode: threadingOverride,
			},
			wantIPCTermLength: logbuffer.TermMinLength,
			wantThreadingMode: threadingOverride,
		},
		{
			name: "parse_error",
			overrides: DriverEnv{
				envIPCTermBufferLength: "not-a-number",
			},
			wantSyntaxErr: true,
			errContains:   envIPCTermBufferLength,
		},
		{
			name: "parse_error_missing_ipc_key",
			overrides: DriverEnv{
				envIPCTermBufferLength: "",
			},
			wantSyntaxErr: true,
			errContains:   envIPCTermBufferLength,
		},
		{
			name: "invalid_term_length_below_min",
			overrides: DriverEnv{
				envIPCTermBufferLength: "32768",
			},
			errContains: "logbuffer: term length below minimum",
		},
		{
			name: "invalid_term_length_not_power_of_two",
			overrides: DriverEnv{
				envIPCTermBufferLength: "123456",
			},
			errContains: "logbuffer: term length not a power of two",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, ipcTermLength, err := buildDriverEnv(dir, tc.overrides)

			if tc.errContains != "" {
				require.Error(t, err)
				if tc.wantSyntaxErr {
					require.ErrorIs(t, err, strconv.ErrSyntax)
				}
				require.Contains(t, err.Error(), tc.errContains)
				require.Nil(t, env)
				require.Zero(t, ipcTermLength)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantIPCTermLength, ipcTermLength)
			require.Equal(t, dir, env[envDir])
			require.Equal(t, defaultDriverEnv[envTermBufferLength], env[envTermBufferLength])
			require.Equal(t, strconv.Itoa(tc.wantIPCTermLength), env[envIPCTermBufferLength])
			require.Equal(t, tc.wantThreadingMode, env[envThreadingMode])
		})
	}
}

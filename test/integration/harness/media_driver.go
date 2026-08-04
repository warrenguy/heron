//go:build integration

// Package harness provides integration-test fixtures for Aeron clients, CNC wiring,
// and media drivers.
//
// HERON_SHARED_MEDIA_DRIVER controls shared driver mode. Unset means enabled; only "0"
// disables shared mode. Any other value keeps shared mode (including "", "false", and
// "1"). When disabled, RunTestMainWithSharedDriver skips start/stop and Shared(t)
// falls back to Start(t).
//
// HERON_SHARED_MEDIA_DRIVER_KEEP is unset by default. When "1", stopSharedDriver skips
// teardown entirely (process and CNC dir remain) for debugging stale CNC state.
//
// Shared driver scope is one long-lived aeronmd per Go test package binary that calls
// RunTestMainWithSharedDriver in TestMain. make test-integration (./test/integration/...)
// starts two such drivers: test/integration/harness (harness + harness_test) and
// test/integration. TestMain wiring lives in shared_testmain_external_test.go and
// shared_testmain_test.go. Packages that call Shared(t)
// should add TestMain plus AssertSharedDriverWiring acceptance tests.
//
// Driver APIs:
//   - StartParallel: calls t.Parallel(), then StartWithConfig with defaults. Use for
//     tests that need per-test driver isolation (driver kill, env overrides, rollover).
//   - Start, StartWithConfig: per-test ephemeral aeronmd; parallel-safe (isolated
//     AERON_DIR, child cmd.Env only). Use StartWithConfig for cfg.Env overrides;
//     call t.Parallel first or use StartParallel for defaults.
//   - JoinExternal: attach to an already-running aeronmd at dir.
//   - Shared, SharedAttachedCnc: package TestMain driver; parallel-safe when tests use
//     isolated stream IDs and do not kill or irreversibly mutate shared driver state.
//   - SharedParallel: calls t.Parallel(), then returns Shared. Default for migrated
//     parallel IPC tests.
package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var termBufferLength = strconv.Itoa(logbuffer.TermMinLength)

const (
	envTermBufferLength    = "AERON_TERM_BUFFER_LENGTH"
	envIPCTermBufferLength = "AERON_IPC_TERM_BUFFER_LENGTH"
	envThreadingMode       = "AERON_THREADING_MODE"
	envDir                 = "AERON_DIR"

	joinExternalCncReadyTimeout = 10 * time.Second
)

type DriverEnv map[string]string

type Config struct {
	Env DriverEnv
}

var defaultDriverEnv = DriverEnv{
	envTermBufferLength:    termBufferLength,
	envIPCTermBufferLength: termBufferLength,
	envThreadingMode:       "SHARED",
}

func RolloverDriverEnv() DriverEnv {
	return DriverEnv{
		envTermBufferLength:    termBufferLength,
		envIPCTermBufferLength: termBufferLength,
	}
}

func buildDriverEnv(dir string, overrides DriverEnv) (DriverEnv, int, error) {
	env := make(DriverEnv, len(defaultDriverEnv)+len(overrides)+1)
	maps.Copy(env, defaultDriverEnv)
	maps.Copy(env, overrides)
	env[envDir] = dir

	ipcTermDataLength, err := strconv.Atoi(env[envIPCTermBufferLength])
	if err != nil {
		return nil, 0, fmt.Errorf(
			"parse %s=%q: %w",
			envIPCTermBufferLength,
			env[envIPCTermBufferLength],
			err,
		)
	}
	if err := logbuffer.CheckTermLength(ipcTermDataLength); err != nil {
		return nil, 0, fmt.Errorf("invalid %s=%d: %w", envIPCTermBufferLength, ipcTermDataLength, err)
	}
	return env, ipcTermDataLength, nil
}

func buildStartedDriverEnv(t testing.TB, dir string) (DriverEnv, int) {
	t.Helper()

	env, ipcTermDataLength, err := buildDriverEnv(dir, nil)
	if err != nil {
		t.Fatalf("harness: %v", err)
	}
	return env, ipcTermDataLength
}

func driverCncPath(dir string) string {
	return filepath.Join(dir, cnc.FileName)
}

func TermDataLengthFromCmdEnv(md *MediaDriver) (int, error) {
	if md == nil {
		return 0, fmt.Errorf("harness: nil media driver")
	}
	if md.ipcTermDataLength <= 0 {
		return 0, fmt.Errorf("harness: ipc term data length not set")
	}
	return md.ipcTermDataLength, nil
}

func buildDriverCmdEnv(merged DriverEnv) []string {
	return mergeCmdEnv(merged)
}

// mergeCmdEnv clones the process environment, drops every key in merged and
// every extraStrip key, then appends the merged pairs.
func mergeCmdEnv(merged map[string]string, extraStrip ...string) []string {
	strip := make(map[string]struct{}, len(merged)+len(extraStrip))
	for key := range merged {
		strip[key] = struct{}{}
	}
	for _, key := range extraStrip {
		strip[key] = struct{}{}
	}

	base := os.Environ()
	out := make([]string, 0, len(base)+len(merged))
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, drop := strip[key]; drop {
			continue
		}
		out = append(out, entry)
	}
	for key, value := range merged {
		out = append(out, key+"="+value)
	}
	return out
}

// repoBinFile returns repo-root/bin/<name> when that path exists. Empty when
// go env GOMOD fails, the module root is empty or os.DevNull, or bin/<name>
// is missing.
func repoBinFile(name string) string {
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return ""
	}
	modRoot := strings.TrimSpace(string(out))
	if modRoot == "" || modRoot == os.DevNull {
		return ""
	}
	path := filepath.Join(filepath.Dir(modRoot), "bin", name)
	if _, err := os.Stat(path); err != nil {
		return ""
	}
	return path
}

var (
	errAeronmdNotFound              = errors.New("aeronmd not found")
	errMediaDriverProcessNotStarted = errors.New("harness: media driver process not started")
)

type MediaDriver struct {
	Dir    string
	cmd    *exec.Cmd
	cancel context.CancelFunc
	pid    int

	ipcTermDataLength int
	external          bool

	exited  chan struct{}
	waitErr error
}

func Start(t testing.TB) *MediaDriver {
	t.Helper()
	return StartWithConfig(t, Config{})
}

// StartParallel calls t.Parallel and starts a per-test ephemeral aeronmd.
// Do not call t.Parallel again in the same test or subtest; Go panics on a second call.
func StartParallel(t *testing.T) *MediaDriver {
	t.Helper()
	t.Parallel()
	return StartWithConfig(t, Config{})
}

// StartWithConfig starts aeronmd with defaultDriverEnv (64 KiB term buffers, SHARED threading),
// merged under cfg.Env (overrides win), and AERON_DIR set to a fresh temp directory.
// Start delegates here with an empty Config.
//
// Safe under t.Parallel when each test gets its own MediaDriver. Env overrides apply to the
// child aeronmd process only; do not use os.Setenv here.
func StartWithConfig(t testing.TB, cfg Config) *MediaDriver {
	t.Helper()
	aeronmd := resolveAeronmdOrSkip(t)
	md, err := startEphemeralMediaDriver(aeronmd, cfg.Env)
	if err != nil {
		t.Fatalf("harness: %v", err)
	}
	t.Cleanup(func() { md.Stop() })
	return md
}

// resolveAeronmdOrSkip resolves the aeronmd binary. Skips the test when the
// binary is absent. Fails the test on any other resolve error.
func resolveAeronmdOrSkip(t testing.TB) string {
	t.Helper()
	path, err := resolveAeronmd()
	if err != nil {
		if errors.Is(err, errAeronmdNotFound) {
			t.Skip("aeronmd not found; run make test-integration (requires Docker)")
			return ""
		}
		t.Fatalf("harness: resolve aeronmd: %v", err)
		return ""
	}
	return path
}

// startEphemeralMediaDriver launches aeronmd in a fresh temp directory.
//
// Concurrency: safe from a worker goroutine.
//
// Ownership: caller owns Stop.
func startEphemeralMediaDriver(aeronmdPath string, overrides DriverEnv) (*MediaDriver, error) {
	return startDriverInTempDir(aeronmdPath, "heron-aeron-*", overrides)
}

// startDriverInTempDir launches aeronmd in a fresh MkdirTemp(pattern)
// directory. The failed-start unwind removes the directory.
func startDriverInTempDir(
	aeronmdPath string,
	pattern string,
	overrides DriverEnv,
) (*MediaDriver, error) {
	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		return nil, fmt.Errorf("mkdir temp: %w", err)
	}
	env, ipcTermDataLength, err := buildDriverEnv(dir, overrides)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	md, err := launchMediaDriver(aeronmdPath, dir, env, ipcTermDataLength)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return md, nil
}

func launchMediaDriver(
	aeronmdPath string,
	dir string,
	env DriverEnv,
	ipcTermDataLength int,
) (*MediaDriver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, aeronmdPath)
	cmd.Dir = dir
	cmd.Env = buildDriverCmdEnv(env)
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start aeronmd: %w", err)
	}

	md := &MediaDriver{
		Dir:               dir,
		cmd:               cmd,
		cancel:            cancel,
		pid:               cmd.Process.Pid,
		ipcTermDataLength: ipcTermDataLength,
		exited:            make(chan struct{}),
	}
	go func() {
		md.waitErr = cmd.Wait()
		close(md.exited)
	}()

	err := cnc.WaitCncReady(
		context.Background(),
		md.CncPath(),
		joinExternalCncReadyTimeout,
	)
	if err != nil {
		md.Stop()
		return nil, fmt.Errorf("%s not ready: %w", cnc.FileName, err)
	}
	return md, nil
}

// JoinExternal attaches to an already-running aeronmd at dir without spawning a process.
//
// ipcTermDataLength on external attach is a client-side assumption from defaultDriverEnv
// (AERON_IPC_TERM_BUFFER_LENGTH), not read from the live driver CNC. External compose
// stacks should set matching term buffer env on every service. Callers must keep
// client and driver term lengths aligned.
func JoinExternal(t testing.TB, dir string) *MediaDriver {
	t.Helper()
	return JoinExternalTimeout(t, dir, joinExternalCncReadyTimeout)
}

// JoinExternalTimeout is JoinExternal with a custom CNC ready poll deadline.
func JoinExternalTimeout(t testing.TB, dir string, readyTimeout time.Duration) *MediaDriver {
	t.Helper()
	require.NotEmpty(t, dir)

	_, ipcTermDataLength := buildStartedDriverEnv(t, dir)

	if err := cnc.WaitCncReady(t.Context(), driverCncPath(dir), readyTimeout); err != nil {
		t.Fatalf("harness: %s not ready: %v", cnc.FileName, err)
	}

	md := &MediaDriver{
		Dir:               dir,
		ipcTermDataLength: ipcTermDataLength,
		external:          true,
	}
	t.Cleanup(func() { md.Stop() })
	return md
}

// AssertCncAlive checks the CNC is readable and the driver consumer heartbeat is fresh.
func AssertCncAlive(t testing.TB, dir string) {
	t.Helper()
	require.NotEmpty(t, dir)

	cncPath := driverCncPath(dir)
	meta, err := cnc.ReadMetadataFile(cncPath)
	require.NoError(t, err, "read cnc metadata")
	if failedCaptureTB(t) {
		return
	}
	require.Positive(t, meta.PID, "cnc pid")

	attach := attachCncPath(t, cncPath)
	if failedCaptureTB(t) {
		return
	}
	AssertConsumerHeartbeatFresh(t, attach)
}

func (md *MediaDriver) Kill() error {
	proc, err := md.aliveProcess()
	if err != nil {
		return err
	}
	killErr := proc.Kill()
	if killErr == nil || errors.Is(killErr, os.ErrProcessDone) {
		md.waitExited()
		return nil
	}
	return killErr
}

func (md *MediaDriver) Stop() {
	if md == nil {
		return
	}
	if md.external {
		md.Dir = ""
		return
	}
	if md.cancel != nil {
		md.cancel()
	}
	md.waitExited()
	md.cmd = nil
	md.cancel = nil
	if md.Dir != "" {
		_ = os.RemoveAll(md.Dir)
		md.Dir = ""
	}
}

func (md *MediaDriver) waitExited() {
	if md.exited != nil {
		<-md.exited
	}
}

func (md *MediaDriver) CncPath() string {
	if md == nil {
		return ""
	}
	return driverCncPath(md.Dir)
}

func (md *MediaDriver) CmdEnvForTest() []string {
	if md == nil || md.cmd == nil {
		return nil
	}
	return md.cmd.Env
}

func (md *MediaDriver) driverProcess() (*os.Process, error) {
	if md == nil || md.cmd == nil || md.cmd.Process == nil {
		return nil, errMediaDriverProcessNotStarted
	}
	return md.cmd.Process, nil
}

func (md *MediaDriver) aliveProcess() (*os.Process, error) {
	if err := md.processExited(); err != nil {
		return nil, fmt.Errorf("harness: aeronmd process %d not alive: %w", md.pid, err)
	}
	return md.driverProcess()
}

func (md *MediaDriver) processExited() error {
	if md.exited == nil {
		return nil
	}
	select {
	case <-md.exited:
		return processDoneErr(md.waitErr)
	default:
		return nil
	}
}

func processDoneErr(waitErr error) error {
	if waitErr == nil {
		return os.ErrProcessDone
	}
	return errors.Join(os.ErrProcessDone, waitErr)
}

func (md *MediaDriver) ProcessPID() (int, error) {
	proc, err := md.aliveProcess()
	if err != nil {
		return 0, err
	}
	return proc.Pid, nil
}

func (md *MediaDriver) aliveError() error {
	_, err := md.aliveProcess()
	return err
}

func (md *MediaDriver) AssertProcessAlive(t testing.TB) {
	t.Helper()
	require.NoError(t, md.aliveError())
}

// AssertAlive checks driver liveness for spawned or external attach paths.
func (md *MediaDriver) AssertAlive(t testing.TB) {
	t.Helper()
	if md.external {
		AssertCncAlive(t, md.Dir)
		return
	}
	md.AssertProcessAlive(t)
}

func resolveAeronmd() (string, error) {
	if path := os.Getenv("AERONMD_PATH"); path != "" {
		if _, err := os.Stat(path); err != nil {
			return "", err
		}
		return path, nil
	}
	return defaultAeronmd()
}

var defaultAeronmd = sync.OnceValues(func() (string, error) {
	if path := repoBinFile("aeronmd"); path != "" {
		return path, nil
	}
	path, err := exec.LookPath("aeronmd")
	if err != nil {
		return "", errAeronmdNotFound
	}
	return path, nil
})

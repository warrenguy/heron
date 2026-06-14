//go:build integration

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

func setupTestDriverEnv(
	t testing.TB,
	dir string,
	overrides DriverEnv,
	onFail func(),
) (DriverEnv, int) {
	t.Helper()

	env := make(DriverEnv, len(defaultDriverEnv)+len(overrides)+1)
	maps.Copy(env, defaultDriverEnv)
	maps.Copy(env, overrides)
	env[envDir] = dir

	ipcTermDataLength, err := strconv.Atoi(env[envIPCTermBufferLength])
	if err != nil {
		if onFail != nil {
			onFail()
		}
		t.Fatalf("harness: parse %s=%q: %v", envIPCTermBufferLength, env[envIPCTermBufferLength], err)
	}
	if err := logbuffer.CheckTermLength(ipcTermDataLength); err != nil {
		if onFail != nil {
			onFail()
		}
		t.Fatalf("harness: invalid %s=%d: %v", envIPCTermBufferLength, ipcTermDataLength, err)
	}
	for key, value := range env {
		t.Setenv(key, value)
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
	strip := make(map[string]struct{}, len(merged)+1)
	for key := range merged {
		strip[key] = struct{}{}
	}
	strip[envDir] = struct{}{}

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

// StartWithConfig starts aeronmd with defaultDriverEnv (64 KiB term buffers, SHARED threading),
// merged under cfg.Env (overrides win), and AERON_DIR set to a fresh temp directory.
// Start delegates here with an empty Config.
func StartWithConfig(t testing.TB, cfg Config) *MediaDriver {
	t.Helper()
	aeronmd, err := resolveAeronmd()
	if err != nil {
		if errors.Is(err, errAeronmdNotFound) {
			t.Skip("aeronmd not found; run make test-integration (requires Docker)")
		}
		t.Fatalf("harness: resolve aeronmd: %v", err)
	}
	dir, err := os.MkdirTemp("", "heron-aeron-*")
	if err != nil {
		t.Fatalf("harness: mkdir temp: %v", err)
	}
	env, ipcTermDataLength := setupTestDriverEnv(t, dir, cfg.Env, func() { _ = os.RemoveAll(dir) })

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, aeronmd)
	cmd.Dir = dir
	cmd.Env = buildDriverCmdEnv(env)
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(dir)
		t.Fatalf("harness: start aeronmd: %v", err)
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

	if err := cnc.WaitCncReady(md.CncPath(), joinExternalCncReadyTimeout); err != nil {
		md.Stop()
		t.Fatalf("harness: %s not ready: %v", cnc.FileName, err)
	}
	t.Cleanup(func() { md.Stop() })
	return md
}

// JoinExternal attaches to an already-running aeronmd at dir without spawning a process.
//
// ipcTermDataLength on external attach is a client-side assumption from defaultDriverEnv
// (AERON_IPC_TERM_BUFFER_LENGTH), not read from the live driver CNC. External compose
// stacks should pin matching term buffer env on every service; callers must keep client
// and driver term lengths aligned.
func JoinExternal(t testing.TB, dir string) *MediaDriver {
	t.Helper()
	return JoinExternalTimeout(t, dir, joinExternalCncReadyTimeout)
}

// JoinExternalTimeout is JoinExternal with a custom CNC ready poll deadline.
func JoinExternalTimeout(t testing.TB, dir string, readyTimeout time.Duration) *MediaDriver {
	t.Helper()
	require.NotEmpty(t, dir)

	_, ipcTermDataLength := setupTestDriverEnv(t, dir, nil, nil)

	if err := cnc.WaitCncReady(driverCncPath(dir), readyTimeout); err != nil {
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
	if path := repoBinAeronmd(); path != "" {
		return path, nil
	}
	path, err := exec.LookPath("aeronmd")
	if err != nil {
		return "", errAeronmdNotFound
	}
	return path, nil
}

var (
	repoBinAeronmdOnce sync.Once
	repoBinAeronmdPath string
)

func repoBinAeronmd() string {
	repoBinAeronmdOnce.Do(func() {
		out, err := exec.Command("go", "env", "GOMOD").Output()
		if err != nil {
			return
		}
		modRoot := strings.TrimSpace(string(out))
		if modRoot == "" || modRoot == os.DevNull {
			return
		}
		path := filepath.Join(filepath.Dir(modRoot), "bin", "aeronmd")
		if _, err := os.Stat(path); err != nil {
			return
		}
		repoBinAeronmdPath = path
	})
	return repoBinAeronmdPath
}

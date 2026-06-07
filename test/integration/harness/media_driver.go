package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
)

const termBufferLength = "65536"

var (
	errAeronmdNotFound              = errors.New("aeronmd not found")
	errMediaDriverProcessNotStarted = errors.New("harness: media driver process not started")
)

type MediaDriver struct {
	Dir    string
	cmd    *exec.Cmd
	cancel context.CancelFunc
	pid    int

	exited  chan struct{}
	waitErr error
}

func Start(t testing.TB) *MediaDriver {
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
	t.Setenv("AERON_DIR", dir)
	t.Setenv("AERON_TERM_BUFFER_LENGTH", termBufferLength)
	t.Setenv("AERON_IPC_TERM_BUFFER_LENGTH", termBufferLength)
	t.Setenv("AERON_THREADING_MODE", "SHARED")

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, aeronmd)
	cmd.Dir = dir
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(dir)
		t.Fatalf("harness: start aeronmd: %v", err)
	}

	md := &MediaDriver{
		Dir:    dir,
		cmd:    cmd,
		cancel: cancel,
		pid:    cmd.Process.Pid,
		exited: make(chan struct{}),
	}
	go func() {
		md.waitErr = cmd.Wait()
		close(md.exited)
	}()

	if err := cnc.WaitCncReady(md.CncPath(), 10*time.Second); err != nil {
		md.Stop()
		t.Fatalf("harness: %s not ready: %v", cnc.FileName, err)
	}
	t.Cleanup(func() { md.Stop() })
	return md
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
	return filepath.Join(md.Dir, cnc.FileName)
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

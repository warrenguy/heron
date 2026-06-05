package harness

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/warrenguy/heron/aeron/cnc"
)

const termBufferLength = "65536"

var errAeronmdNotFound = errors.New("aeronmd not found")

type MediaDriver struct {
	Dir    string
	cmd    *exec.Cmd
	cancel context.CancelFunc
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

	md := &MediaDriver{Dir: dir, cmd: cmd, cancel: cancel}
	if err := cnc.WaitCncReady(md.CncPath(), 10*time.Second); err != nil {
		md.Stop()
		t.Fatalf("harness: %s not ready: %v", cnc.FileName, err)
	}
	t.Cleanup(func() { md.Stop() })
	return md
}

func (md *MediaDriver) Stop() {
	if md == nil {
		return
	}
	if md.cancel != nil {
		md.cancel()
	}
	if md.cmd != nil {
		_ = md.cmd.Wait()
	}
	if md.Dir != "" {
		_ = os.RemoveAll(md.Dir)
		md.Dir = ""
	}
}

func (md *MediaDriver) CncPath() string {
	if md == nil {
		return ""
	}
	return filepath.Join(md.Dir, cnc.FileName)
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

//go:build integration

package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	envArchiveDir                = "HERON_ARCHIVE_DIR"
	envArchiveControlChannel     = "HERON_ARCHIVE_CONTROL_CHANNEL"
	envArchiveReplicationChannel = "HERON_ARCHIVE_REPLICATION_CHANNEL"
	envArchiveJar                = "HERON_ARCHIVE_JAR"

	// ArchiveReadyMarker is the ready-file name under ArchiveNode.Dir.
	ArchiveReadyMarker  = ".heron-archive-ready"
	archiveReadyTimeout = 60 * time.Second

	archiveNodeMainClass = "heron.archivejava.ArchiveNode"
	archiveAddOpens      = "java.base/jdk.internal.misc=ALL-UNNAMED"

	// archiveRuntimeMissing is the host-fix phrase for a missing Archive jar or
	// Java runtime. StartArchiveNode skips with it. RequireArchiveRuntime fails
	// with it.
	archiveRuntimeMissing = "archive jar not found; run make test-cluster-archive"
)

var (
	errArchiveJarNotFound       = errors.New("archive jar not found")
	errArchiveExitedBeforeReady = errors.New("archive process exited before ready")
)

// ArchiveNode is one Archive server process co-located with one media driver.
//
// The fixture owns the process. StartArchiveNode registers Stop on t.Cleanup.
// The consensus module never starts an Archive. The module connects a control
// client to an Archive that is already running.
//
// Concurrency: invoker-only. Every method runs on the test goroutine.
//
// Lifetime:
//   - t.Cleanup is LIFO. When the caller registers MediaDriver.Start first,
//     then StartArchiveNode, teardown stops the Archive before its driver.
//   - Stop is idempotent and nil-safe.
//   - Dir and ArchiveID stay readable after Stop for failure dumps.
//
// The caller must not stop the media driver before the Archive.
type ArchiveNode struct {
	// Dir is the per-member archive data directory. The catalog, the segment
	// files, and the ready marker live here.
	Dir string

	// ArchiveID is the server archive id from the ready marker, as decimal text.
	// Use for failure dumps only.
	//
	// Do not equate ArchiveID with archive.Client.ArchiveID. That accessor is
	// session-shaped and is not a dump substitute for the marker value.
	ArchiveID string

	cancel context.CancelFunc
	exited chan struct{}
}

// ArchiveNodeConfig is the per-member Archive configuration the fixture owns.
type ArchiveNodeConfig struct {
	// AeronDir is the media driver directory this Archive attaches to.
	// Required. Pass MediaDriver.Dir.
	AeronDir string

	// ControlChannel is the full channel URI the Archive binds for UDP control,
	// for example aeron:udp?endpoint=127.0.0.1:25001. Required. The endpoint must
	// be unique per Archive on the host.
	//
	// The Go control client does not use this channel. The client connects over the
	// local IPC control channel. The server binds a UDP control channel
	// unconditionally.
	ControlChannel string
}

// StartArchiveNode launches one Archive server against AeronDir and returns
// once the ready marker carries an archive id.
//
// Behaviour: creates a fresh archive data directory under t.TempDir. Launches
// the server as a child process. Polls the ready marker. Skips the test when
// the Archive jar or a Java runtime is absent. Fails the test when the server
// exits early or does not become ready within archiveReadyTimeout.
//
// Concurrency: invoker-only.
//
// Ownership: the returned ArchiveNode owns the child process. t.Cleanup stops
// the process. The caller must not stop the media driver before the Archive.
func StartArchiveNode(t testing.TB, cfg ArchiveNodeConfig) *ArchiveNode {
	t.Helper()
	require.NotEmpty(t, cfg.AeronDir, "ArchiveNodeConfig.AeronDir")
	require.NotEmpty(t, cfg.ControlChannel, "ArchiveNodeConfig.ControlChannel")

	javaPath, jarPath := resolveArchiveRuntimeOrSkip(t)

	node, err := startArchiveNodeProcess(
		cfg.AeronDir,
		t.TempDir(),
		cfg.ControlChannel,
		javaPath,
		jarPath,
	)
	if err != nil {
		t.Fatalf("harness: %v", err)
	}
	t.Cleanup(func() { node.Stop() })
	return node
}

// resolveArchiveRuntimeOrSkip resolves the Java runtime and Archive jar for a
// live Archive start. Skips the test when either is absent. Fails the test on
// any other jar resolve error.
func resolveArchiveRuntimeOrSkip(t testing.TB) (javaPath, jarPath string) {
	t.Helper()
	jarPath, err := resolveArchiveJar()
	if err != nil {
		if errors.Is(err, errArchiveJarNotFound) {
			t.Skip(archiveRuntimeMissing)
			return "", ""
		}
		t.Fatalf("harness: resolve archive jar: %v", err)
		return "", ""
	}
	javaPath, err = exec.LookPath("java")
	if err != nil {
		t.Skip(archiveRuntimeMissing)
		return "", ""
	}
	return javaPath, jarPath
}

// startArchiveNodeProcess launches one Archive server and waits for the ready
// marker.
//
// Concurrency: safe from a worker goroutine. Does not use testing.TB.
//
// Ownership: caller owns Stop.
func startArchiveNodeProcess(
	aeronDir string,
	archiveDir string,
	controlChannel string,
	javaPath string,
	jarPath string,
) (*ArchiveNode, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(
		ctx,
		javaPath,
		"--add-opens",
		archiveAddOpens,
		"-cp",
		jarPath,
		archiveNodeMainClass,
	)
	cmd.Env = buildArchiveCmdEnv(aeronDir, archiveDir, controlChannel)

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start archive node: %w", err)
	}

	node := &ArchiveNode{
		Dir:    archiveDir,
		cancel: cancel,
		exited: make(chan struct{}),
	}
	go func() {
		_ = cmd.Wait()
		close(node.exited)
	}()

	readyPath := filepath.Join(archiveDir, ArchiveReadyMarker)
	data, err := waitArchiveReady(node, readyPath, archiveReadyTimeout)
	if err != nil {
		node.Stop()
		return nil, fmt.Errorf("archive ready marker: %w", err)
	}
	node.ArchiveID = string(data)
	return node, nil
}

// Stop cancels the Archive child process and waits for exit.
//
// Behaviour: idempotent and nil-safe. Keeps Dir and ArchiveID for failure dumps.
// Does not remove the archive directory. t.TempDir owns cleanup.
//
// Concurrency: invoker-only.
func (n *ArchiveNode) Stop() {
	if n == nil {
		return
	}
	if n.cancel != nil {
		n.cancel()
		n.cancel = nil
	}
	if n.exited != nil {
		<-n.exited
		n.exited = nil
	}
}

// waitArchiveReady polls the ready marker and fails fast if the child exits.
func waitArchiveReady(node *ArchiveNode, path string, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-node.exited:
			return nil, errArchiveExitedBeforeReady
		default:
		}

		raw, err := os.ReadFile(path)
		if err == nil {
			data := bytes.TrimSpace(raw)
			if len(data) > 0 {
				return data, nil
			}
		}

		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("%s not ready within %s", path, timeout)
		}

		select {
		case <-node.exited:
			return nil, errArchiveExitedBeforeReady
		case <-ticker.C:
		}
	}
}

func buildArchiveCmdEnv(aeronDir, archiveDir, controlChannel string) []string {
	// Strip every Archive env key the Java process honors, including replication
	// the launcher does not set. A parent export must not redirect the child.
	return mergeCmdEnv(map[string]string{
		envDir:                   aeronDir,
		envArchiveDir:            archiveDir,
		envArchiveControlChannel: controlChannel,
	}, envArchiveReplicationChannel)
}

func resolveArchiveJar() (string, error) {
	if path := os.Getenv(envArchiveJar); path != "" {
		if _, err := os.Stat(path); err != nil {
			return "", err
		}
		return path, nil
	}
	if forceDefaultArchiveJarMiss.Load() {
		return "", errArchiveJarNotFound
	}
	return defaultArchiveJar()
}

// defaultArchiveJar is immutable OnceValues. Tests set forceDefaultArchiveJarMiss
// for the miss path. Parallel resolve peers must not observe a rewritten package
// function pointer.
var (
	defaultArchiveJar = sync.OnceValues(func() (string, error) {
		if path := repoBinFile("heron-archive-java.jar"); path != "" {
			return path, nil
		}
		return "", errArchiveJarNotFound
	})
	forceDefaultArchiveJarMiss atomic.Bool
)

// forceDefaultArchiveJarMissForTest makes resolveArchiveJar report
// errArchiveJarNotFound for the default jar until tb finishes.
func forceDefaultArchiveJarMissForTest(tb testing.TB) {
	tb.Helper()
	forceDefaultArchiveJarMiss.Store(true)
	tb.Cleanup(func() { forceDefaultArchiveJarMiss.Store(false) })
}

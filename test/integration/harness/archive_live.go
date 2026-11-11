//go:build integration

package harness

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/archive/codecs"
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

// AwaitRecordingStop polls the Archive on the driver in aeronDir until
// recordingID holds a stop position at or past stopPosition.
//
// Returns the descriptor that met the stop position.
//
// Returns an error when:
//   - the client or Archive connect fails
//   - ctx ends first; the error wraps the last list error
func AwaitRecordingStop(
	ctx context.Context,
	aeronDir string,
	recordingID, stopPosition int64,
) (codecs.RecordingDescriptorFields, error) {
	transport, err := aeron.Connect(ctx, aeron.Config{AeronDir: aeronDir})
	if err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	defer func() { _ = transport.Close() }()

	archiveClient, err := archive.Connect(ctx, archive.Config{Aeron: transport, AeronDir: aeronDir})
	if err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	defer func() { _ = archiveClient.Close() }()

	var lastListErr error

	for {
		if err := ctx.Err(); err != nil {
			return codecs.RecordingDescriptorFields{}, fmt.Errorf(
				"list recording %d: %w",
				recordingID,
				errors.Join(err, lastListErr),
			)
		}

		desc, listErr := archiveClient.ListRecording(ctx, recordingID)
		lastListErr = listErr

		if listErr == nil && desc.StopPosition >= stopPosition {
			return desc, nil
		}

		time.Sleep(time.Millisecond)
	}
}

//go:build testhooks

package election

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/internal/testhook"
	"github.com/warrenguy/heron/mmap"
)

// initialiseOnCreateHooks is keyed by node-state file path. Concurrent
// OpenNodeStateFile create peers must not observe another test's hook.
var (
	initialiseOnCreateHooks = testhook.NewRegistry[string, func([]byte, int32) (int, int, error)]()
	truncateNodeStateHooks  = testhook.NewRegistry[string, func(*os.File, int64) error]()
	mapNodeStateHooks       = testhook.NewRegistry[
		string,
		func(*os.File, int) (*mmap.Mapping, error),
	]()
)

func callInitialiseOnCreate(path string, buf []byte, version int32) (int, int, error) {
	if hook := initialiseOnCreateHooks.Load(path); hook != nil {
		return hook(buf, version)
	}
	return initialiseOnCreate(buf, version)
}

func truncateNodeStateFile(file *os.File, size int64) error {
	if hook := truncateNodeStateHooks.Load(file.Name()); hook != nil {
		return hook(file, size)
	}
	return file.Truncate(size)
}

func mapNodeStateFile(file *os.File, length int) (*mmap.Mapping, error) {
	if hook := mapNodeStateHooks.Load(file.Name()); hook != nil {
		return hook(file, length)
	}
	return mmap.MapOpenedFile(file, length)
}

// SetInitialiseOnCreateHookForTest installs a create-initialise hook for path.
//
// path must be the node-state path OpenNodeStateFile will create.
func SetInitialiseOnCreateHookForTest(
	tb testing.TB,
	path string,
	hook func([]byte, int32) (int, int, error),
) {
	tb.Helper()
	testhook.SetForTest(tb, initialiseOnCreateHooks, path, hook)
}

// SetTruncateNodeStateHookForTest installs a truncate hook for path.
//
// path must be the node-state path OpenNodeStateFile will create.
func SetTruncateNodeStateHookForTest(
	tb testing.TB,
	path string,
	hook func(*os.File, int64) error,
) {
	tb.Helper()
	testhook.SetForTest(tb, truncateNodeStateHooks, path, hook)
}

// SetMapNodeStateHookForTest installs a map hook for path.
//
// path must be the node-state path OpenNodeStateFile will create.
func SetMapNodeStateHookForTest(
	tb testing.TB,
	path string,
	hook func(*os.File, int) (*mmap.Mapping, error),
) {
	tb.Helper()
	testhook.SetForTest(tb, mapNodeStateHooks, path, hook)
}

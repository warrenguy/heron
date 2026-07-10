//go:build testhooks

package election

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/mmap"
)

var (
	initialiseOnCreateHook func([]byte, int32) (int, int, error)
	truncateNodeStateHook  func(*os.File, int64) error
	mapNodeStateHook       func(*os.File, int) (*mmap.Mapping, error)
)

func callInitialiseOnCreate(buf []byte, version int32) (int, int, error) {
	if initialiseOnCreateHook != nil {
		return initialiseOnCreateHook(buf, version)
	}
	return initialiseOnCreate(buf, version)
}

func truncateNodeStateFile(file *os.File, size int64) error {
	if truncateNodeStateHook != nil {
		return truncateNodeStateHook(file, size)
	}
	return file.Truncate(size)
}

func mapNodeStateFile(file *os.File, length int) (*mmap.Mapping, error) {
	if mapNodeStateHook != nil {
		return mapNodeStateHook(file, length)
	}
	return mmap.MapOpenedFile(file, length)
}

func SetInitialiseOnCreateHook(
	hook func([]byte, int32) (int, int, error),
) func() {
	previous := initialiseOnCreateHook
	initialiseOnCreateHook = hook
	return func() { initialiseOnCreateHook = previous }
}

func SetTruncateNodeStateHook(hook func(*os.File, int64) error) func() {
	previous := truncateNodeStateHook
	truncateNodeStateHook = hook
	return func() { truncateNodeStateHook = previous }
}

func SetMapNodeStateHook(hook func(*os.File, int) (*mmap.Mapping, error)) func() {
	previous := mapNodeStateHook
	mapNodeStateHook = hook
	return func() { mapNodeStateHook = previous }
}

func SetInitialiseOnCreateHookForTest(
	tb testing.TB,
	hook func([]byte, int32) (int, int, error),
) {
	tb.Helper()
	restore := SetInitialiseOnCreateHook(hook)
	tb.Cleanup(restore)
}

func SetTruncateNodeStateHookForTest(tb testing.TB, hook func(*os.File, int64) error) {
	tb.Helper()
	restore := SetTruncateNodeStateHook(hook)
	tb.Cleanup(restore)
}

func SetMapNodeStateHookForTest(tb testing.TB, hook func(*os.File, int) (*mmap.Mapping, error)) {
	tb.Helper()
	restore := SetMapNodeStateHook(hook)
	tb.Cleanup(restore)
}

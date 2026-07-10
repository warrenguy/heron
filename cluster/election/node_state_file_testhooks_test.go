//go:build testhooks

package election_test

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/mmap"
)

func TestOpenNodeStateFile_createInitialiseFailureRemovesPartialFile(t *testing.T) {
	dir := t.TempDir()
	path := nodeStatePath(dir)

	election.SetInitialiseOnCreateHookForTest(t, func(
		_ []byte,
		_ int32,
	) (int, int, error) {
		return 0, 0, election.ErrCorruptNodeState
	})

	_, err := election.OpenNodeStateFile(dir, true, recordinglog.FileSyncNone)
	require.ErrorIs(t, err, election.ErrCorruptNodeState)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpenNodeStateFile_truncateFailureRemovesPartialFile(t *testing.T) {
	dir := t.TempDir()
	path := nodeStatePath(dir)

	election.SetTruncateNodeStateHookForTest(t, func(*os.File, int64) error {
		return errors.New("truncate failed")
	})

	_, err := election.OpenNodeStateFile(dir, true, recordinglog.FileSyncNone)
	require.Error(t, err)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpenNodeStateFile_mapFailureRemovesPartialFile(t *testing.T) {
	dir := t.TempDir()
	path := nodeStatePath(dir)

	election.SetMapNodeStateHookForTest(t, func(*os.File, int) (*mmap.Mapping, error) {
		return nil, errors.New("mmap failed")
	})

	_, err := election.OpenNodeStateFile(dir, true, recordinglog.FileSyncNone)
	require.Error(t, err)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

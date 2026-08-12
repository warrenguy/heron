//go:build integration

package harness

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildArchiveCmdEnv_table(t *testing.T) {
	const (
		parentAeronDir    = "/tmp/heron-aeron-parent"
		mergedAeronDir    = "/tmp/heron-aeron-merged"
		parentArchiveDir  = "/tmp/heron-archive-parent"
		mergedArchiveDir  = "/tmp/heron-archive-merged"
		parentControl     = "aeron:udp?endpoint=127.0.0.1:1"
		mergedControl     = "aeron:udp?endpoint=127.0.0.1:2"
		parentReplication = "aeron:udp?endpoint=127.0.0.1:3"
		passThroughKey    = "HERON_BUILD_ARCHIVE_CMD_ENV_TEST"
		passThroughValue  = "keep-me"
	)

	tests := []struct {
		name    string
		parent  map[string]string
		aeron   string
		archive string
		control string
		want    map[string]string
		absent  []string
	}{
		{
			name: "strips stale parent AERON_DIR and archive keys",
			parent: map[string]string{
				envDir:                       parentAeronDir,
				envArchiveDir:                parentArchiveDir,
				envArchiveControlChannel:     parentControl,
				envArchiveReplicationChannel: parentReplication,
			},
			aeron:   mergedAeronDir,
			archive: mergedArchiveDir,
			control: mergedControl,
			want: map[string]string{
				envDir:                   mergedAeronDir,
				envArchiveDir:            mergedArchiveDir,
				envArchiveControlChannel: mergedControl,
			},
			absent: []string{envArchiveReplicationChannel},
		},
		{
			name: "passes through unrelated parent keys",
			parent: map[string]string{
				passThroughKey: passThroughValue,
			},
			aeron:   mergedAeronDir,
			archive: mergedArchiveDir,
			control: mergedControl,
			want: map[string]string{
				envDir:                   mergedAeronDir,
				envArchiveDir:            mergedArchiveDir,
				envArchiveControlChannel: mergedControl,
				passThroughKey:           passThroughValue,
			},
			absent: []string{envArchiveReplicationChannel},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.parent {
				t.Setenv(key, value)
			}

			got := buildArchiveCmdEnv(tc.aeron, tc.archive, tc.control)
			cmdEnv := envSliceToMap(t, got)

			for key, wantValue := range tc.want {
				require.Equal(t, wantValue, cmdEnv[key])
			}
			for _, key := range tc.absent {
				_, present := cmdEnv[key]
				require.False(t, present, "expected %s stripped from child env", key)
			}
		})
	}
}

func TestWaitArchiveReady(t *testing.T) {
	t.Parallel()

	t.Run("ready marker present", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, ArchiveReadyMarker)
		require.NoError(t, os.WriteFile(path, []byte("  42\n"), 0o600))

		node := &ArchiveNode{exited: make(chan struct{})}
		data, err := waitArchiveReady(node, path, time.Second)
		require.NoError(t, err)
		require.Equal(t, []byte("42"), data)
	})

	t.Run("process exited before ready", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, ArchiveReadyMarker)

		exited := make(chan struct{})
		close(exited)
		node := &ArchiveNode{exited: exited}

		data, err := waitArchiveReady(node, path, time.Second)
		require.ErrorIs(t, err, errArchiveExitedBeforeReady)
		require.Nil(t, data)
	})

	t.Run("timeout without marker", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, ArchiveReadyMarker)

		node := &ArchiveNode{exited: make(chan struct{})}
		data, err := waitArchiveReady(node, path, 20*time.Millisecond)
		require.Error(t, err)
		require.NotErrorIs(t, err, errArchiveExitedBeforeReady)
		require.Contains(t, err.Error(), "not ready")
		require.Nil(t, data)
	})

	t.Run("timeout whitespace-only marker", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, ArchiveReadyMarker)
		require.NoError(t, os.WriteFile(path, []byte("  \n"), 0o600))

		node := &ArchiveNode{exited: make(chan struct{})}
		data, err := waitArchiveReady(node, path, 20*time.Millisecond)
		require.Error(t, err)
		require.NotErrorIs(t, err, errArchiveExitedBeforeReady)
		require.Contains(t, err.Error(), "not ready")
		require.Nil(t, data)
	})
}

func TestResolveArchiveJar(t *testing.T) {
	t.Run("env override missing path returns error", func(t *testing.T) {
		t.Setenv(envArchiveJar, filepath.Join(t.TempDir(), "missing-archive.jar"))

		path, err := resolveArchiveJar()
		require.Error(t, err)
		require.Empty(t, path)
		require.NotErrorIs(t, err, errArchiveJarNotFound)
	})

	t.Run("default miss returns errArchiveJarNotFound", func(t *testing.T) {
		// Force flag rather than a rewritten defaultArchiveJar. Parallel resolve peers
		// must not sample a half-installed override.
		forceDefaultArchiveJarMissForTest(t)
		t.Setenv(envArchiveJar, "")

		path, err := resolveArchiveJar()
		require.ErrorIs(t, err, errArchiveJarNotFound)
		require.Empty(t, path)
	})

	t.Run("env override existing path returns path", func(t *testing.T) {
		dir := t.TempDir()
		jar := filepath.Join(dir, "custom-archive.jar")
		require.NoError(t, os.WriteFile(jar, []byte("jar"), 0o600))
		t.Setenv(envArchiveJar, jar)

		path, err := resolveArchiveJar()
		require.NoError(t, err)
		require.Equal(t, jar, path)
	})
}

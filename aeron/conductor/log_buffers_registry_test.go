package conductor_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func writeMinimalLogFileForRegistryTest(tb testing.TB, path string) {
	tb.Helper()

	const termLength = logbuffer.TermMinLength
	const pageSize = logbuffer.PageMinSize
	computed, err := logbuffer.ComputeLogLength(termLength, pageSize)
	require.NoError(tb, err)

	buf := make([]byte, computed)
	metaStart := logbuffer.MetaDataOffset(termLength)
	meta := aeronatomic.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])
	meta.PutInt32Release(logbuffer.OffsetTermLength, termLength)
	meta.PutInt32(logbuffer.OffsetPageSize, pageSize)
	meta.PutInt32(logbuffer.OffsetMTULength, 1408)
	meta.PutInt32(logbuffer.OffsetInitialTermID, 7)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	require.NoError(tb, os.WriteFile(path, buf, 0o644))
}

func TestLogBuffersRegistry_Acquire_errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		logPath func(t *testing.T) string
		wantErr error
	}{
		{
			name:    "emptyPath",
			logPath: func(t *testing.T) string { return "" },
			wantErr: conductor.ErrLogFileNameEmpty,
		},
		{
			name: "missingFile",
			logPath: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "missing.log")
			},
			wantErr: conductor.ErrLogBufferMapFailed,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			registry := conductor.NewLogBuffersRegistry("")
			_, err := registry.Acquire(testCase.logPath(t))
			require.ErrorIs(t, err, testCase.wantErr)
		})
	}
}

func TestLogBuffersRegistry_Acquire_dedupPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, dir string) (pathA, pathB string)
	}{
		{
			name: "symlink",
			setup: func(t *testing.T, dir string) (string, string) {
				targetPath := filepath.Join(dir, "target.log")
				writeMinimalLogFileForRegistryTest(t, targetPath)

				aliasPath := filepath.Join(dir, "alias.log")
				require.NoError(t, os.Symlink(targetPath, aliasPath))
				return aliasPath, targetPath
			},
		},
		{
			name: "canonical",
			setup: func(t *testing.T, dir string) (string, string) {
				logPath := filepath.Join(dir, "shared.log")
				writeMinimalLogFileForRegistryTest(t, logPath)
				return logPath, filepath.Join(dir, ".", "shared.log")
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			pathA, pathB := testCase.setup(t, dir)
			registry := conductor.NewLogBuffersRegistry(dir)

			first, err := registry.Acquire(pathA)
			require.NoError(t, err)
			second, err := registry.Acquire(pathB)
			require.NoError(t, err)
			require.Same(t, first, second)
			require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, pathA))
			require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, pathB))
		})
	}
}

func TestLogBuffersRegistry_Acquire_corruptLogFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "corrupt.log")
	require.NoError(t, os.WriteFile(logPath, []byte{0, 0, 0}, 0o644))

	registry := conductor.NewLogBuffersRegistry(dir)
	_, err := registry.Acquire(logPath)
	require.ErrorIs(t, err, conductor.ErrLogBufferMapFailed)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_Release_panics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T) (*conductor.LogBuffersRegistry, string)
		release   func(registry *conductor.LogBuffersRegistry, logPath string)
		wantPanic string
	}{
		{
			name: "nonZeroLinger",
			setup: func(t *testing.T) (*conductor.LogBuffersRegistry, string) {
				return conductor.NewLogBuffersRegistry(""), "any.log"
			},
			release: func(registry *conductor.LogBuffersRegistry, logPath string) {
				registry.Release(logPath, 1)
			},
			wantPanic: "conductor: log buffer linger is not implemented",
		},
		{
			name: "overRelease",
			setup: func(t *testing.T) (*conductor.LogBuffersRegistry, string) {
				logPath := filepath.Join(t.TempDir(), "over-release.log")
				registry := conductor.NewLogBuffersRegistry("")
				require.NoError(
					t,
					conductor.SeedLogBuffersRegistryForTest(
						registry,
						logPath,
						conductor.NewSyntheticLogBuffersForTest(t),
					),
				)
				require.NoError(
					t,
					conductor.SetLogBuffersRegistryRefCountForTest(registry, logPath, 0),
				)
				return registry, logPath
			},
			release: func(registry *conductor.LogBuffersRegistry, logPath string) {
				registry.Release(logPath, 0)
			},
			wantPanic: "conductor: log buffer release underflow",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			registry, logPath := testCase.setup(t)
			require.PanicsWithValue(
				t,
				testCase.wantPanic,
				func() { testCase.release(registry, logPath) },
			)
		})
	}
}

func TestSetLogBuffersRegistryRefCountForTest_errors(t *testing.T) {
	t.Parallel()

	t.Run("resolveFailure", func(t *testing.T) {
		t.Parallel()
		registry := conductor.NewLogBuffersRegistry("")
		err := conductor.SetLogBuffersRegistryRefCountForTest(registry, "", 0)
		require.ErrorIs(t, err, conductor.ErrLogFileNameEmpty)
	})

	t.Run("missingEntry", func(t *testing.T) {
		t.Parallel()
		logPath := filepath.Join(t.TempDir(), "not-seeded.log")
		registry := conductor.NewLogBuffersRegistry("")
		err := conductor.SetLogBuffersRegistryRefCountForTest(registry, logPath, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in registry")
	})
}

func TestLogBuffersRegistry_releaseResolvedKey_skipsReResolve(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "shared.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	registry := conductor.NewLogBuffersRegistry(dir)
	_, err := registry.Acquire(logPath)
	require.NoError(t, err)
	resolvedKey, err := conductor.ResolveLogFileKeyForTest(registry, logPath)
	require.NoError(t, err)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	registry.Release("/tmp/\xff.log", 0)
	require.Equal(
		t,
		int32(1),
		conductor.LogBuffersRegistryRefCountByResolvedKeyForTest(registry, resolvedKey),
	)

	conductor.ReleaseResolvedKeyForTest(registry, resolvedKey, 0)
	require.Zero(
		t,
		conductor.LogBuffersRegistryRefCountByResolvedKeyForTest(registry, resolvedKey),
	)
}

func TestLogBuffersRegistry_Release_unknownPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "shared.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	registry := conductor.NewLogBuffersRegistry(dir)
	_, err := registry.Acquire(logPath)
	require.NoError(t, err)

	// Footgun: unknown paths are an intentional silent no-op, unlike over-release
	// which panics. Callers must pair Release with a prior Acquire.
	require.NotPanics(t, func() { registry.Release(filepath.Join(dir, "missing.log"), 0) })
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_lookupLogFile_resolvedKeyOnly(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "shared.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	registry := conductor.NewLogBuffersRegistry(dir)
	_, err := registry.Acquire(logPath)
	require.NoError(t, err)

	aliasPath := filepath.Join(dir, ".", "shared.log")
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, aliasPath))
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, aliasPath))

	registry.Release(aliasPath, 0)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_SeedForTest_refcount(t *testing.T) {
	t.Parallel()

	logBuffers := conductor.NewSyntheticLogBuffersForTest(t)

	logPath := filepath.Join(t.TempDir(), "seeded.log")
	registry := conductor.NewLogBuffersRegistry("")
	require.NoError(t, conductor.SeedLogBuffersRegistryForTest(registry, logPath, logBuffers))

	acquired, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.Same(t, logBuffers, acquired)
	require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	registry.Release(logPath, 0)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_SeedForTest_errors(t *testing.T) {
	t.Parallel()

	registry := conductor.NewLogBuffersRegistry("")
	logBuffers := conductor.NewSyntheticLogBuffersForTest(t)

	require.EqualError(
		t,
		conductor.SeedLogBuffersRegistryForTest(nil, "path", logBuffers),
		"conductor: log buffers registry is nil",
	)
	require.EqualError(
		t,
		conductor.SeedLogBuffersRegistryForTest(registry, "", logBuffers),
		"conductor: log file name is empty",
	)
	require.EqualError(
		t,
		conductor.SeedLogBuffersRegistryForTest(registry, "path", nil),
		"conductor: log buffers is nil",
	)
	require.NoError(t, conductor.SeedLogBuffersRegistryForTest(registry, "path", logBuffers))
	require.EqualError(
		t,
		conductor.SeedLogBuffersRegistryForTest(registry, "path", logBuffers),
		`conductor: log file "path" already registered`,
	)
}

func TestLogBuffersRegistry_AcquireRelease_refcount(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "shared.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	registry := conductor.NewLogBuffersRegistry(dir)

	first, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	second, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.Same(t, first.MappedLogBuffer(), second.MappedLogBuffer())
	require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	registry.Release(logPath, 0)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	registry.Release(logPath, 0)
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_Release_emptyPathNoOp(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "shared.log")
	writeMinimalLogFileForRegistryTest(t, logPath)

	registry := conductor.NewLogBuffersRegistry(dir)
	_, err := registry.Acquire(logPath)
	require.NoError(t, err)
	registry.Release(logPath, 0)
	require.NotPanics(t, func() { registry.Release("", 0) })
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

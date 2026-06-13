//go:build integration

package harness

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestStartWithConfig_defaultsApplied(t *testing.T) {
	md := Start(t)
	require.NotEmpty(t, md.Dir)

	require.Equal(t, defaultDriverEnv[envTermBufferLength], os.Getenv(envTermBufferLength))
	require.Equal(t, defaultDriverEnv[envIPCTermBufferLength], os.Getenv(envIPCTermBufferLength))
	require.Equal(t, defaultDriverEnv[envThreadingMode], os.Getenv(envThreadingMode))
	require.Equal(t, md.Dir, os.Getenv(envDir))
}

func TestStartWithConfig_envOverride(t *testing.T) {
	const ipcTermBufferOverride = "131072"
	md := StartWithConfig(t, Config{
		Env: DriverEnv{
			envIPCTermBufferLength: ipcTermBufferOverride,
		},
	})
	require.NotEmpty(t, md.Dir)

	require.Equal(t, defaultDriverEnv[envTermBufferLength], os.Getenv(envTermBufferLength))
	require.Equal(t, ipcTermBufferOverride, os.Getenv(envIPCTermBufferLength))
	require.Equal(t, defaultDriverEnv[envThreadingMode], os.Getenv(envThreadingMode))
	require.Equal(t, md.Dir, os.Getenv(envDir))
}

func TestStartWithConfig_childCmdEnvOverride(t *testing.T) {
	const ipcTermBufferOverride = "262144"
	// Stale parent IPC stripping is covered by TestBuildDriverCmdEnv_table.

	md := StartWithConfig(t, Config{
		Env: DriverEnv{
			envIPCTermBufferLength: ipcTermBufferOverride,
		},
	})
	require.NotEmpty(t, md.Dir)

	cmdEnv := envSliceToMap(t, md.CmdEnvForTest())
	require.Equal(t, ipcTermBufferOverride, cmdEnv[envIPCTermBufferLength])
	require.Equal(t, md.Dir, cmdEnv[envDir])
}

func TestBuildDriverCmdEnv_table(t *testing.T) {
	const parentDir = "/tmp/heron-aeron-parent"
	const mergedDir = "/tmp/heron-aeron-merged"
	const passThroughKey = "HERON_BUILD_DRIVER_CMD_ENV_TEST"
	const passThroughValue = "keep-me"

	tests := []struct {
		name    string
		parent  map[string]string
		merged  DriverEnv
		want    map[string]string
		noStale map[string]string
	}{
		{
			name: "strips stale parent AERON_DIR",
			parent: map[string]string{
				envDir: parentDir,
			},
			merged: DriverEnv{
				envDir: mergedDir,
			},
			want: map[string]string{
				envDir: mergedDir,
			},
			noStale: map[string]string{
				envDir: parentDir,
			},
		},
		{
			name: "merged IPC override replaces parent",
			parent: map[string]string{
				envIPCTermBufferLength: "131072",
			},
			merged: DriverEnv{
				envIPCTermBufferLength: termBufferLength,
				envDir:                 "/tmp/heron-aeron-rollover",
			},
			want: map[string]string{
				envIPCTermBufferLength: termBufferLength,
				envDir:                 "/tmp/heron-aeron-rollover",
			},
			noStale: map[string]string{
				envIPCTermBufferLength: "131072",
			},
		},
		{
			name: "passes through unrelated parent keys",
			parent: map[string]string{
				passThroughKey: passThroughValue,
			},
			merged: DriverEnv{
				envThreadingMode: "SHARED",
			},
			want: map[string]string{
				envThreadingMode: "SHARED",
				passThroughKey:   passThroughValue,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.parent {
				t.Setenv(key, value)
			}

			got := buildDriverCmdEnv(tc.merged)
			cmdEnv := envSliceToMap(t, got)

			for key, wantValue := range tc.want {
				require.Equal(t, wantValue, cmdEnv[key])
			}
			for key, staleValue := range tc.noStale {
				require.NotEqual(t, staleValue, cmdEnv[key])
			}
		})
	}
}

func TestRolloverDriverEnv_matchesTermMinLength(t *testing.T) {
	env := RolloverDriverEnv()

	require.Equal(t, termBufferLength, env[envIPCTermBufferLength])
	require.Equal(t, termBufferLength, env[envTermBufferLength])
}

func TestTermDataLengthFromCmdEnv_matchesStartedDriver(t *testing.T) {
	md := StartWithConfig(t, Config{Env: RolloverDriverEnv()})

	termLen, err := TermDataLengthFromCmdEnv(md)
	require.NoError(t, err)
	require.Equal(t, logbuffer.TermMinLength, termLen)

	cmdEnv := envSliceToMap(t, md.CmdEnvForTest())
	require.Equal(t, termBufferLength, cmdEnv[envIPCTermBufferLength])
}

func envSliceToMap(t *testing.T, entries []string) map[string]string {
	t.Helper()

	out := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		require.True(t, ok, "malformed env entry %q", entry)
		if prior, exists := out[key]; exists {
			require.Failf(t, "duplicate env key %q: first=%q second=%q", key, prior, value)
		}
		out[key] = value
	}
	return out
}

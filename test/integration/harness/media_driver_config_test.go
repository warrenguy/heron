//go:build integration

package harness

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStartWithConfig_defaultsApplied(t *testing.T) {
	md := Start(t)
	require.NotEmpty(t, md.Dir)

	require.Equal(t, defaultDriverEnv[envTermBufferLength], os.Getenv(envTermBufferLength))
	require.Equal(t, defaultDriverEnv[envIPCTermBufferLength], os.Getenv(envIPCTermBufferLength))
	require.Equal(t, defaultDriverEnv[envThreadingMode], os.Getenv(envThreadingMode))
	require.Equal(t, md.Dir, os.Getenv(envDir))
}

// Child cmd.Env is not asserted here.
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

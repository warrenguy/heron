//go:build testhooks

package archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
)

func concludeConfig(
	t *testing.T,
	cfg archive.Config,
) (archive.Config, *aeron.Client, bool) {
	t.Helper()
	concluded, clientOut, owns, err := archive.ConcludeConfigForTest(t.Context(), cfg)
	require.NoError(t, err)
	return concluded, clientOut, owns
}

func concludedConfig(
	t *testing.T,
	mutate func(*archive.Config),
) archive.Config {
	t.Helper()
	cfg := normalizedConfig(t, mutate)
	cfg.Aeron = &aeron.Client{}
	concluded, _, _ := concludeConfig(t, cfg)
	return concluded
}

func TestConcludeConfig_injectedAeron(t *testing.T) {
	injected := &aeron.Client{}
	cfg := normalizedConfig(t, func(cfg *archive.Config) {
		cfg.Aeron = injected
	})

	concluded, clientOut, owns := concludeConfig(t, cfg)
	require.False(t, owns)
	require.Same(t, injected, clientOut)
	require.Same(t, injected, concluded.Aeron)
}

func TestConcludeConfig_ownedClientFactory(t *testing.T) {
	aeronDir := t.TempDir()
	owned := &aeron.Client{}
	archive.SetConnectConstructionHooksForTest(
		t,
		aeronDir,
		func(cfg archive.Config) (*aeron.Client, error) {
			return owned, nil
		},
	)

	cfg := normalizedConfig(t, func(cfg *archive.Config) {
		cfg.AeronDir = ""
		cfg.AeronConfig = aeron.Config{AeronDir: aeronDir}
	})

	concluded, clientOut, owns := concludeConfig(t, cfg)
	require.True(t, owns)
	require.Same(t, owned, clientOut)
	require.Same(t, owned, concluded.Aeron)
}

func TestConcludeConfig_invalidResponseChannel(t *testing.T) {
	cfg := normalizedConfig(t, func(cfg *archive.Config) {
		withRemoteControl(cfg)
		cfg.ControlResponseChannel = "not-a-channel"
		cfg.Aeron = &aeron.Client{}
	})

	_, _, _, err := archive.ConcludeConfigForTest(t.Context(), cfg)
	require.EqualError(t, err, "archive: invalid control response channel: not-a-channel")
}

func TestConcludeConfig_udpRejoinFalseRemote(t *testing.T) {
	concluded := concludedConfig(t, withRemoteControl)

	require.Contains(t, concluded.EffectiveControlResponseChannel(), "rejoin=false")
}

func TestConcludeConfig_ipcSkipsRejoinFalse(t *testing.T) {
	concluded := concludedConfig(t, nil)

	channel := concluded.EffectiveControlResponseChannel()
	require.Equal(t, archive.DefaultLocalControlChannel, channel)
	require.NotContains(t, channel, "rejoin=false")
}

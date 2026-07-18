package archive_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
)

func validConfig(aeronDir string) archive.Config {
	return archive.Config{
		AeronDir:            aeronDir,
		CredentialsSupplier: archive.NullCredentialsSupplier{},
		IdleStrategy:        archive.NewDefaultIdleStrategy(),
	}
}

func withRemoteControl(cfg *archive.Config) {
	*cfg = cfg.WithRemoteControl()
}

func withRemoteRequestChannel(channel string) func(*archive.Config) {
	return func(cfg *archive.Config) {
		withRemoteControl(cfg)
		cfg.ControlRequestChannel = channel
	}
}

func withRemoteResponseChannel(channel string) func(*archive.Config) {
	return func(cfg *archive.Config) {
		withRemoteControl(cfg)
		cfg.ControlResponseChannel = channel
	}
}

func normalizedConfig(
	t *testing.T,
	mutate func(*archive.Config),
) archive.Config {
	t.Helper()
	cfg := validConfig(t.TempDir())
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg.Normalize()
}

func TestNormalizeConfig_defaultsIpc(t *testing.T) {
	cfg := normalizedConfig(t, nil)

	require.Equal(t, archive.DefaultLocalControlChannel, cfg.LocalControlChannel)
	require.Equal(t, archive.DefaultControlRequestChannel, cfg.ControlRequestChannel)
	require.Equal(t, archive.DefaultControlResponseChannel, cfg.ControlResponseChannel)
	require.Equal(t, archive.DefaultLocalControlStreamID, cfg.LocalControlStreamID)
	require.Equal(t, archive.DefaultControlRequestStreamID, cfg.ControlRequestStreamID)
	require.Equal(t, archive.DefaultControlResponseStreamID, cfg.ControlResponseStreamID)
	require.Equal(t, archive.DefaultConnectTimeout, cfg.ConnectTimeout)
	require.Equal(t, archive.DefaultControlResponseTimeout, cfg.ControlResponseTimeout)
	require.Equal(t, archive.DefaultControlOfferRetryTimeout, cfg.ControlOfferRetryTimeout)
	require.Equal(t, archive.DefaultMessageRetryAttempts, cfg.MessageRetryAttempts)
	require.Equal(t, "archive-client", cfg.ClientName)
	require.IsType(t, archive.NullCredentialsSupplier{}, cfg.CredentialsSupplier)
	require.NotNil(t, cfg.IdleStrategy)
}

func TestNormalizeConfig_withRemoteControl(t *testing.T) {
	cfg := normalizedConfig(t, withRemoteControl)

	require.Empty(t, cfg.LocalControlChannel)
	require.Equal(t, archive.DefaultControlRequestChannel, cfg.ControlRequestChannel)
	require.Equal(t, archive.DefaultControlResponseChannel, cfg.ControlResponseChannel)
}

func TestNormalizeConfig_preservesExplicitLocalControl(t *testing.T) {
	cfg := normalizedConfig(t, func(cfg *archive.Config) {
		cfg.LocalControlChannel = archive.DefaultLocalControlChannel
		cfg.LocalControlStreamID = 77
	})

	require.Equal(t, archive.DefaultLocalControlChannel, cfg.LocalControlChannel)
	require.Equal(t, int32(77), cfg.LocalControlStreamID)
}

func TestNormalizeConfig_idempotent(t *testing.T) {
	initial := validConfig(t.TempDir())
	once := initial.Normalize()
	twice := once.Normalize()

	require.Equal(t, once, twice)
}

func TestValidateConfig_acceptsDefault(t *testing.T) {
	cfg := normalizedConfig(t, nil)

	require.NoError(t, cfg.ValidateForConnect())
}

func TestValidateConfig_errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*archive.Config)
		wantErr string
	}{
		{
			name:    "invalid request channel",
			mutate:  withRemoteRequestChannel("not-a-channel"),
			wantErr: "archive: invalid control request channel: not-a-channel",
		},
		{
			name:    "unsupported request media",
			mutate:  withRemoteRequestChannel("aeron:unknown"),
			wantErr: "archive: unknown control request channel not supported; use ipc, ipcx, or udp",
		},
		{
			name:    "invalid response channel",
			mutate:  withRemoteResponseChannel("not-a-channel"),
			wantErr: "archive: invalid control response channel: not-a-channel",
		},
		{
			name:    "unsupported response media",
			mutate:  withRemoteResponseChannel("aeron:unknown"),
			wantErr: "archive: unknown control response channel not supported; use ipc, ipcx, or udp",
		},
		{
			name: "connect timeout negative",
			mutate: func(cfg *archive.Config) {
				cfg.ConnectTimeout = -time.Second
			},
			wantErr: "archive: ConnectTimeout must be positive",
		},
		{
			name: "message timeout negative",
			mutate: func(cfg *archive.Config) {
				cfg.ControlResponseTimeout = -time.Second
			},
			wantErr: "archive: ControlResponseTimeout must be positive",
		},
		{
			name: "client name too long",
			mutate: func(cfg *archive.Config) {
				cfg.ClientName = strings.Repeat("x", archive.MaxClientNameLength+1)
			},
			wantErr: "archive: ClientName length must be <= 100",
		},
		{
			name: "missing aeron dir",
			mutate: func(cfg *archive.Config) {
				cfg.AeronDir = ""
				cfg.AeronConfig = aeron.Config{}
			},
			wantErr: "archive: AeronDir is required when Aeron is not set",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := normalizedConfig(t, testCase.mutate)
			err := cfg.ValidateForConnect()
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestValidateConfig_aeronConfigDirAccepted(t *testing.T) {
	cfg := normalizedConfig(t, func(cfg *archive.Config) {
		cfg.AeronDir = ""
		cfg.AeronConfig = aeron.Config{AeronDir: t.TempDir()}
	})

	require.NoError(t, cfg.ValidateForConnect())
}

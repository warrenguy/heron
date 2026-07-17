package archive_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
)

func validContext(aeronDir string) archive.Context {
	return archive.Context{
		AeronDir:            aeronDir,
		CredentialsSupplier: archive.NullCredentialsSupplier{},
		IdleStrategy:        archive.NewDefaultIdleStrategy(),
	}
}

func withRemoteControl(archiveCtx *archive.Context) {
	*archiveCtx = archiveCtx.WithRemoteControl()
}

func withRemoteRequestChannel(channel string) func(*archive.Context) {
	return func(archiveCtx *archive.Context) {
		withRemoteControl(archiveCtx)
		archiveCtx.ControlRequestChannel = channel
	}
}

func withRemoteResponseChannel(channel string) func(*archive.Context) {
	return func(archiveCtx *archive.Context) {
		withRemoteControl(archiveCtx)
		archiveCtx.ControlResponseChannel = channel
	}
}

func normalizedContext(
	t *testing.T,
	mutate func(*archive.Context),
) archive.Context {
	t.Helper()
	archiveCtx := validContext(t.TempDir())
	if mutate != nil {
		mutate(&archiveCtx)
	}
	return archiveCtx.Normalize()
}

func TestNormalizeContext_defaultsIpc(t *testing.T) {
	archiveCtx := normalizedContext(t, nil)

	require.Equal(t, archive.DefaultLocalControlChannel, archiveCtx.LocalControlChannel)
	require.Equal(t, archive.DefaultControlRequestChannel, archiveCtx.ControlRequestChannel)
	require.Equal(t, archive.DefaultControlResponseChannel, archiveCtx.ControlResponseChannel)
	require.Equal(t, archive.DefaultLocalControlStreamID, archiveCtx.LocalControlStreamID)
	require.Equal(t, archive.DefaultControlRequestStreamID, archiveCtx.ControlRequestStreamID)
	require.Equal(t, archive.DefaultControlResponseStreamID, archiveCtx.ControlResponseStreamID)
	require.Equal(t, archive.DefaultConnectTimeout, archiveCtx.ConnectTimeout)
	require.Equal(t, archive.DefaultControlResponseTimeout, archiveCtx.ControlResponseTimeout)
	require.Equal(t, archive.DefaultControlOfferRetryTimeout, archiveCtx.ControlOfferRetryTimeout)
	require.Equal(t, archive.DefaultMessageRetryAttempts, archiveCtx.MessageRetryAttempts)
	require.Equal(t, "archive-client", archiveCtx.ClientName)
	require.IsType(t, archive.NullCredentialsSupplier{}, archiveCtx.CredentialsSupplier)
	require.NotNil(t, archiveCtx.IdleStrategy)
}

func TestNormalizeContext_withRemoteControl(t *testing.T) {
	archiveCtx := normalizedContext(t, withRemoteControl)

	require.Empty(t, archiveCtx.LocalControlChannel)
	require.Equal(t, archive.DefaultControlRequestChannel, archiveCtx.ControlRequestChannel)
	require.Equal(t, archive.DefaultControlResponseChannel, archiveCtx.ControlResponseChannel)
}

func TestNormalizeContext_preservesExplicitLocalControl(t *testing.T) {
	archiveCtx := normalizedContext(t, func(archiveCtx *archive.Context) {
		archiveCtx.LocalControlChannel = archive.DefaultLocalControlChannel
		archiveCtx.LocalControlStreamID = 77
	})

	require.Equal(t, archive.DefaultLocalControlChannel, archiveCtx.LocalControlChannel)
	require.Equal(t, int32(77), archiveCtx.LocalControlStreamID)
}

func TestNormalizeContext_idempotent(t *testing.T) {
	initial := validContext(t.TempDir())
	once := initial.Normalize()
	twice := once.Normalize()

	require.Equal(t, once, twice)
}

func TestValidateContext_acceptsDefault(t *testing.T) {
	archiveCtx := normalizedContext(t, nil)

	require.NoError(t, archiveCtx.ValidateForConnect())
}

func TestValidateContext_errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*archive.Context)
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
			mutate: func(archiveCtx *archive.Context) {
				archiveCtx.ConnectTimeout = -time.Second
			},
			wantErr: "archive: ConnectTimeout must be positive",
		},
		{
			name: "message timeout negative",
			mutate: func(archiveCtx *archive.Context) {
				archiveCtx.ControlResponseTimeout = -time.Second
			},
			wantErr: "archive: ControlResponseTimeout must be positive",
		},
		{
			name: "client name too long",
			mutate: func(archiveCtx *archive.Context) {
				archiveCtx.ClientName = strings.Repeat("x", archive.MaxClientNameLength+1)
			},
			wantErr: "archive: ClientName length must be <= 100",
		},
		{
			name: "missing aeron dir",
			mutate: func(archiveCtx *archive.Context) {
				archiveCtx.AeronDir = ""
				archiveCtx.AeronContext = aeron.Context{}
			},
			wantErr: "archive: AeronDir is required when Aeron is not set",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			archiveCtx := normalizedContext(t, testCase.mutate)
			err := archiveCtx.ValidateForConnect()
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestValidateContext_aeronContextDirAccepted(t *testing.T) {
	archiveCtx := normalizedContext(t, func(archiveCtx *archive.Context) {
		archiveCtx.AeronDir = ""
		archiveCtx.AeronContext = aeron.Context{AeronDir: t.TempDir()}
	})

	require.NoError(t, archiveCtx.ValidateForConnect())
}

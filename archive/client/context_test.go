package client_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/client"
)

func validArchiveContext(aeronDir string) client.ArchiveContext {
	return client.ArchiveContext{
		AeronDir:            aeronDir,
		CredentialsSupplier: client.NullCredentialsSupplier{},
		IdleStrategy:        client.NewDefaultIdleStrategy(),
	}
}

func withRemoteControl(ctx *client.ArchiveContext) {
	*ctx = ctx.WithRemoteControl()
}

func withRemoteRequestChannel(channel string) func(*client.ArchiveContext) {
	return func(ctx *client.ArchiveContext) {
		withRemoteControl(ctx)
		ctx.ControlRequestChannel = channel
	}
}

func withRemoteResponseChannel(channel string) func(*client.ArchiveContext) {
	return func(ctx *client.ArchiveContext) {
		withRemoteControl(ctx)
		ctx.ControlResponseChannel = channel
	}
}

func normalizedArchiveContext(
	t *testing.T,
	mutate func(*client.ArchiveContext),
) client.ArchiveContext {
	t.Helper()
	ctx := validArchiveContext(t.TempDir())
	if mutate != nil {
		mutate(&ctx)
	}
	return ctx.Normalize()
}

func TestNormalizeArchiveContext_defaultsIpc(t *testing.T) {
	ctx := normalizedArchiveContext(t, nil)

	require.Equal(t, client.DefaultLocalControlChannel, ctx.LocalControlChannel)
	require.Equal(t, client.DefaultControlRequestChannel, ctx.ControlRequestChannel)
	require.Equal(t, client.DefaultControlResponseChannel, ctx.ControlResponseChannel)
	require.Equal(t, client.DefaultLocalControlStreamID, ctx.LocalControlStreamID)
	require.Equal(t, client.DefaultControlRequestStreamID, ctx.ControlRequestStreamID)
	require.Equal(t, client.DefaultControlResponseStreamID, ctx.ControlResponseStreamID)
	require.Equal(t, client.DefaultConnectTimeout, ctx.ConnectTimeout)
	require.Equal(t, client.DefaultControlResponseTimeout, ctx.ControlResponseTimeout)
	require.Equal(t, client.DefaultControlOfferRetryTimeout, ctx.ControlOfferRetryTimeout)
	require.Equal(t, client.DefaultMessageRetryAttempts, ctx.MessageRetryAttempts)
	require.Equal(t, "archive-client", ctx.ClientName)
	require.IsType(t, client.NullCredentialsSupplier{}, ctx.CredentialsSupplier)
	require.NotNil(t, ctx.IdleStrategy)
}

func TestNormalizeArchiveContext_withRemoteControl(t *testing.T) {
	ctx := normalizedArchiveContext(t, withRemoteControl)

	require.Empty(t, ctx.LocalControlChannel)
	require.Equal(t, client.DefaultControlRequestChannel, ctx.ControlRequestChannel)
	require.Equal(t, client.DefaultControlResponseChannel, ctx.ControlResponseChannel)
}

func TestNormalizeArchiveContext_preservesExplicitLocalControl(t *testing.T) {
	ctx := normalizedArchiveContext(t, func(ctx *client.ArchiveContext) {
		ctx.LocalControlChannel = client.DefaultLocalControlChannel
		ctx.LocalControlStreamID = 77
	})

	require.Equal(t, client.DefaultLocalControlChannel, ctx.LocalControlChannel)
	require.Equal(t, int32(77), ctx.LocalControlStreamID)
}

func TestNormalizeArchiveContext_idempotent(t *testing.T) {
	initial := validArchiveContext(t.TempDir())
	once := initial.Normalize()
	twice := once.Normalize()

	require.Equal(t, once, twice)
}

func TestValidateArchiveContext_acceptsDefault(t *testing.T) {
	ctx := normalizedArchiveContext(t, nil)

	require.NoError(t, ctx.ValidateForConnect())
}

func TestValidateArchiveContext_errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*client.ArchiveContext)
		wantErr string
	}{
		{
			name:    "invalid request channel",
			mutate:  withRemoteRequestChannel("not-a-channel"),
			wantErr: "archive client: invalid control request channel: not-a-channel",
		},
		{
			name:    "unsupported request media",
			mutate:  withRemoteRequestChannel("aeron:unknown"),
			wantErr: "archive client: unknown control request channel not supported; use ipc, ipcx, or udp",
		},
		{
			name:    "invalid response channel",
			mutate:  withRemoteResponseChannel("not-a-channel"),
			wantErr: "archive client: invalid control response channel: not-a-channel",
		},
		{
			name:    "unsupported response media",
			mutate:  withRemoteResponseChannel("aeron:unknown"),
			wantErr: "archive client: unknown control response channel not supported; use ipc, ipcx, or udp",
		},
		{
			name: "connect timeout negative",
			mutate: func(ctx *client.ArchiveContext) {
				ctx.ConnectTimeout = -time.Second
			},
			wantErr: "archive client: ConnectTimeout must be positive",
		},
		{
			name: "message timeout negative",
			mutate: func(ctx *client.ArchiveContext) {
				ctx.ControlResponseTimeout = -time.Second
			},
			wantErr: "archive client: ControlResponseTimeout must be positive",
		},
		{
			name: "client name too long",
			mutate: func(ctx *client.ArchiveContext) {
				ctx.ClientName = strings.Repeat("x", client.MaxClientNameLength+1)
			},
			wantErr: "archive client: ClientName length must be <= 100",
		},
		{
			name: "missing aeron dir",
			mutate: func(ctx *client.ArchiveContext) {
				ctx.AeronDir = ""
				ctx.AeronContext = aeron.Context{}
			},
			wantErr: "archive client: AeronDir is required when Aeron is not set",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := normalizedArchiveContext(t, testCase.mutate)
			err := ctx.ValidateForConnect()
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestValidateArchiveContext_aeronContextDirAccepted(t *testing.T) {
	ctx := normalizedArchiveContext(t, func(ctx *client.ArchiveContext) {
		ctx.AeronDir = ""
		ctx.AeronContext = aeron.Context{AeronDir: t.TempDir()}
	})

	require.NoError(t, ctx.ValidateForConnect())
}

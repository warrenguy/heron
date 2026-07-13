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

func withRemoteControl(ctx *archive.Context) {
	*ctx = ctx.WithRemoteControl()
}

func withRemoteRequestChannel(channel string) func(*archive.Context) {
	return func(ctx *archive.Context) {
		withRemoteControl(ctx)
		ctx.ControlRequestChannel = channel
	}
}

func withRemoteResponseChannel(channel string) func(*archive.Context) {
	return func(ctx *archive.Context) {
		withRemoteControl(ctx)
		ctx.ControlResponseChannel = channel
	}
}

func normalizedContext(
	t *testing.T,
	mutate func(*archive.Context),
) archive.Context {
	t.Helper()
	ctx := validContext(t.TempDir())
	if mutate != nil {
		mutate(&ctx)
	}
	return ctx.Normalize()
}

func TestNormalizeContext_defaultsIpc(t *testing.T) {
	ctx := normalizedContext(t, nil)

	require.Equal(t, archive.DefaultLocalControlChannel, ctx.LocalControlChannel)
	require.Equal(t, archive.DefaultControlRequestChannel, ctx.ControlRequestChannel)
	require.Equal(t, archive.DefaultControlResponseChannel, ctx.ControlResponseChannel)
	require.Equal(t, archive.DefaultLocalControlStreamID, ctx.LocalControlStreamID)
	require.Equal(t, archive.DefaultControlRequestStreamID, ctx.ControlRequestStreamID)
	require.Equal(t, archive.DefaultControlResponseStreamID, ctx.ControlResponseStreamID)
	require.Equal(t, archive.DefaultConnectTimeout, ctx.ConnectTimeout)
	require.Equal(t, archive.DefaultControlResponseTimeout, ctx.ControlResponseTimeout)
	require.Equal(t, archive.DefaultControlOfferRetryTimeout, ctx.ControlOfferRetryTimeout)
	require.Equal(t, archive.DefaultMessageRetryAttempts, ctx.MessageRetryAttempts)
	require.Equal(t, "archive-client", ctx.ClientName)
	require.IsType(t, archive.NullCredentialsSupplier{}, ctx.CredentialsSupplier)
	require.NotNil(t, ctx.IdleStrategy)
}

func TestNormalizeContext_withRemoteControl(t *testing.T) {
	ctx := normalizedContext(t, withRemoteControl)

	require.Empty(t, ctx.LocalControlChannel)
	require.Equal(t, archive.DefaultControlRequestChannel, ctx.ControlRequestChannel)
	require.Equal(t, archive.DefaultControlResponseChannel, ctx.ControlResponseChannel)
}

func TestNormalizeContext_preservesExplicitLocalControl(t *testing.T) {
	ctx := normalizedContext(t, func(ctx *archive.Context) {
		ctx.LocalControlChannel = archive.DefaultLocalControlChannel
		ctx.LocalControlStreamID = 77
	})

	require.Equal(t, archive.DefaultLocalControlChannel, ctx.LocalControlChannel)
	require.Equal(t, int32(77), ctx.LocalControlStreamID)
}

func TestNormalizeContext_idempotent(t *testing.T) {
	initial := validContext(t.TempDir())
	once := initial.Normalize()
	twice := once.Normalize()

	require.Equal(t, once, twice)
}

func TestValidateContext_acceptsDefault(t *testing.T) {
	ctx := normalizedContext(t, nil)

	require.NoError(t, ctx.ValidateForConnect())
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
			mutate: func(ctx *archive.Context) {
				ctx.ConnectTimeout = -time.Second
			},
			wantErr: "archive: ConnectTimeout must be positive",
		},
		{
			name: "message timeout negative",
			mutate: func(ctx *archive.Context) {
				ctx.ControlResponseTimeout = -time.Second
			},
			wantErr: "archive: ControlResponseTimeout must be positive",
		},
		{
			name: "client name too long",
			mutate: func(ctx *archive.Context) {
				ctx.ClientName = strings.Repeat("x", archive.MaxClientNameLength+1)
			},
			wantErr: "archive: ClientName length must be <= 100",
		},
		{
			name: "missing aeron dir",
			mutate: func(ctx *archive.Context) {
				ctx.AeronDir = ""
				ctx.AeronContext = aeron.Context{}
			},
			wantErr: "archive: AeronDir is required when Aeron is not set",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := normalizedContext(t, testCase.mutate)
			err := ctx.ValidateForConnect()
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestValidateContext_aeronContextDirAccepted(t *testing.T) {
	ctx := normalizedContext(t, func(ctx *archive.Context) {
		ctx.AeronDir = ""
		ctx.AeronContext = aeron.Context{AeronDir: t.TempDir()}
	})

	require.NoError(t, ctx.ValidateForConnect())
}

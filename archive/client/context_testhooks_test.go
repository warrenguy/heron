//go:build testhooks

package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/client"
)

func concludeArchiveContext(
	t *testing.T,
	ctx client.ArchiveContext,
) (client.ArchiveContext, *aeron.Client, bool) {
	t.Helper()
	concluded, clientOut, owns, err := client.ConcludeArchiveContextForTest(ctx)
	require.NoError(t, err)
	return concluded, clientOut, owns
}

func concludedArchiveContext(
	t *testing.T,
	mutate func(*client.ArchiveContext),
) client.ArchiveContext {
	t.Helper()
	ctx := normalizedArchiveContext(t, mutate)
	ctx.Aeron = &aeron.Client{}
	concluded, _, _ := concludeArchiveContext(t, ctx)
	return concluded
}

func TestConcludeArchiveContext_injectedAeron(t *testing.T) {
	injected := &aeron.Client{}
	ctx := normalizedArchiveContext(t, func(ctx *client.ArchiveContext) {
		ctx.Aeron = injected
	})

	concluded, clientOut, owns := concludeArchiveContext(t, ctx)
	require.False(t, owns)
	require.Same(t, injected, clientOut)
	require.Same(t, injected, concluded.Aeron)
}

func TestConcludeArchiveContext_ownedClientFactory(t *testing.T) {
	aeronDir := t.TempDir()
	owned := &aeron.Client{}
	client.SetConnectConstructionHooksForTest(
		t,
		aeronDir,
		func(ctx client.ArchiveContext) (*aeron.Client, error) {
			return owned, nil
		},
	)

	ctx := normalizedArchiveContext(t, func(ctx *client.ArchiveContext) {
		ctx.AeronDir = ""
		ctx.AeronContext = aeron.Context{AeronDir: aeronDir}
	})

	concluded, clientOut, owns := concludeArchiveContext(t, ctx)
	require.True(t, owns)
	require.Same(t, owned, clientOut)
	require.Same(t, owned, concluded.Aeron)
}

func TestConcludeArchiveContext_invalidResponseChannel(t *testing.T) {
	ctx := normalizedArchiveContext(t, func(ctx *client.ArchiveContext) {
		withRemoteControl(ctx)
		ctx.ControlResponseChannel = "not-a-channel"
		ctx.Aeron = &aeron.Client{}
	})

	_, _, _, err := client.ConcludeArchiveContextForTest(ctx)
	require.EqualError(t, err, "archive client: invalid control response channel: not-a-channel")
}

func TestConcludeArchiveContext_udpRejoinFalseRemote(t *testing.T) {
	concluded := concludedArchiveContext(t, withRemoteControl)

	require.Contains(t, concluded.EffectiveControlResponseChannel(), "rejoin=false")
}

func TestConcludeArchiveContext_ipcSkipsRejoinFalse(t *testing.T) {
	concluded := concludedArchiveContext(t, nil)

	channel := concluded.EffectiveControlResponseChannel()
	require.Equal(t, client.DefaultLocalControlChannel, channel)
	require.NotContains(t, channel, "rejoin=false")
}

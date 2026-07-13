//go:build testhooks

package archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
)

func concludeContext(
	t *testing.T,
	ctx archive.Context,
) (archive.Context, *aeron.Client, bool) {
	t.Helper()
	concluded, clientOut, owns, err := archive.ConcludeContextForTest(ctx)
	require.NoError(t, err)
	return concluded, clientOut, owns
}

func concludedContext(
	t *testing.T,
	mutate func(*archive.Context),
) archive.Context {
	t.Helper()
	ctx := normalizedContext(t, mutate)
	ctx.Aeron = &aeron.Client{}
	concluded, _, _ := concludeContext(t, ctx)
	return concluded
}

func TestConcludeContext_injectedAeron(t *testing.T) {
	injected := &aeron.Client{}
	ctx := normalizedContext(t, func(ctx *archive.Context) {
		ctx.Aeron = injected
	})

	concluded, clientOut, owns := concludeContext(t, ctx)
	require.False(t, owns)
	require.Same(t, injected, clientOut)
	require.Same(t, injected, concluded.Aeron)
}

func TestConcludeContext_ownedClientFactory(t *testing.T) {
	aeronDir := t.TempDir()
	owned := &aeron.Client{}
	archive.SetConnectConstructionHooksForTest(
		t,
		aeronDir,
		func(ctx archive.Context) (*aeron.Client, error) {
			return owned, nil
		},
	)

	ctx := normalizedContext(t, func(ctx *archive.Context) {
		ctx.AeronDir = ""
		ctx.AeronContext = aeron.Context{AeronDir: aeronDir}
	})

	concluded, clientOut, owns := concludeContext(t, ctx)
	require.True(t, owns)
	require.Same(t, owned, clientOut)
	require.Same(t, owned, concluded.Aeron)
}

func TestConcludeContext_invalidResponseChannel(t *testing.T) {
	ctx := normalizedContext(t, func(ctx *archive.Context) {
		withRemoteControl(ctx)
		ctx.ControlResponseChannel = "not-a-channel"
		ctx.Aeron = &aeron.Client{}
	})

	_, _, _, err := archive.ConcludeContextForTest(ctx)
	require.EqualError(t, err, "archive: invalid control response channel: not-a-channel")
}

func TestConcludeContext_udpRejoinFalseRemote(t *testing.T) {
	concluded := concludedContext(t, withRemoteControl)

	require.Contains(t, concluded.EffectiveControlResponseChannel(), "rejoin=false")
}

func TestConcludeContext_ipcSkipsRejoinFalse(t *testing.T) {
	concluded := concludedContext(t, nil)

	channel := concluded.EffectiveControlResponseChannel()
	require.Equal(t, archive.DefaultLocalControlChannel, channel)
	require.NotContains(t, channel, "rejoin=false")
}

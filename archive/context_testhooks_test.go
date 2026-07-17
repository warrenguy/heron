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
	archiveCtx archive.Context,
) (archive.Context, *aeron.Client, bool) {
	t.Helper()
	concluded, clientOut, owns, err := archive.ConcludeContextForTest(t.Context(), archiveCtx)
	require.NoError(t, err)
	return concluded, clientOut, owns
}

func concludedContext(
	t *testing.T,
	mutate func(*archive.Context),
) archive.Context {
	t.Helper()
	archiveCtx := normalizedContext(t, mutate)
	archiveCtx.Aeron = &aeron.Client{}
	concluded, _, _ := concludeContext(t, archiveCtx)
	return concluded
}

func TestConcludeContext_injectedAeron(t *testing.T) {
	injected := &aeron.Client{}
	archiveCtx := normalizedContext(t, func(archiveCtx *archive.Context) {
		archiveCtx.Aeron = injected
	})

	concluded, clientOut, owns := concludeContext(t, archiveCtx)
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
		func(archiveCtx archive.Context) (*aeron.Client, error) {
			return owned, nil
		},
	)

	archiveCtx := normalizedContext(t, func(archiveCtx *archive.Context) {
		archiveCtx.AeronDir = ""
		archiveCtx.AeronContext = aeron.Context{AeronDir: aeronDir}
	})

	concluded, clientOut, owns := concludeContext(t, archiveCtx)
	require.True(t, owns)
	require.Same(t, owned, clientOut)
	require.Same(t, owned, concluded.Aeron)
}

func TestConcludeContext_invalidResponseChannel(t *testing.T) {
	archiveCtx := normalizedContext(t, func(archiveCtx *archive.Context) {
		withRemoteControl(archiveCtx)
		archiveCtx.ControlResponseChannel = "not-a-channel"
		archiveCtx.Aeron = &aeron.Client{}
	})

	_, _, _, err := archive.ConcludeContextForTest(t.Context(), archiveCtx)
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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
)

func TestArchiveConnectConfig(t *testing.T) {
	t.Parallel()

	client := &aeron.Client{}
	cfg := Config{
		ArchiveControlRequestChannel:  "aeron:ipc?alias=request",
		ArchiveControlResponseChannel: "aeron:ipc?alias=response",
		AeronConfig:                   aeron.Config{AeronDir: "/dev/shm/aeron-test"},
	}

	got := archiveConnectConfig(cfg, client).Normalize()
	require.Equal(t, cfg.ArchiveControlRequestChannel, got.ControlRequestChannel)
	require.Equal(t, cfg.ArchiveControlResponseChannel, got.ControlResponseChannel)
	require.Same(t, client, got.Aeron)
	require.Equal(t, cfg.AeronConfig.AeronDir, got.AeronConfig.AeronDir)
	require.Equal(t, archive.DefaultControlRequestStreamID, got.ControlRequestStreamID)
	require.Equal(t, int32(120), got.ControlResponseStreamID)
	require.Empty(t, got.LocalControlChannel)
	require.False(t, got.OwnsAeronClient())
	require.NoError(t, got.ValidateForConnect())
}

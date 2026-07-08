//go:build integration

package harness_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
	"github.com/warrenguy/heron/ringbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestMediaDriver_Kill(t *testing.T) {
	t.Parallel()
	md := harness.Start(t)

	pid, err := md.ProcessPID()
	require.NoError(t, err)

	require.NoError(t, md.Kill())

	err = md.Kill()
	require.ErrorIs(t, err, os.ErrProcessDone)
	require.Contains(t, err.Error(), fmt.Sprintf("process %d not alive", pid))
}

func TestMediaDriver(t *testing.T) {
	md := harness.Shared(t)

	t.Run("SharedDriverAvailable", func(t *testing.T) {
		require.NotEmpty(t, md.Dir)
	})

	t.Run("ParseLiveCncMetadata", func(t *testing.T) {
		meta, err := cnc.ReadMetadataFile(md.CncPath())
		require.NoError(t, err, "read metadata")
		require.Positive(t, meta.ToDriverBufferLength)
		require.Positive(t, meta.ToClientsBufferLength)
		require.Positive(t, meta.PID)
		require.Positive(t, meta.FilePageSize)
		require.True(t, atomicbuffer.IsPowerOfTwo(int(meta.FilePageSize)))
		require.Positive(t, meta.ClientLivenessTimeoutNs)
	})

	t.Run("MapLiveCncFile", func(t *testing.T) {
		mapped, err := cnc.MapCncFile(md.CncPath())
		require.NoError(t, err, "map cnc file")
		defer func() { _ = mapped.Close() }()

		meta := mapped.Metadata()
		readMeta, err := cnc.ReadMetadataFile(md.CncPath())
		require.NoError(t, err, "read metadata")
		require.Equal(t, readMeta, meta)
		_, err = ringbuffer.CheckCapacity(
			int(meta.ToDriverBufferLength),
			ringbuffer.MinCapacity,
		)
		require.NoError(t, err, "to-driver CheckCapacity")
		_, err = broadcast.CheckCapacity(int(meta.ToClientsBufferLength))
		require.NoError(t, err, "to-clients CheckCapacity")
		err = mapped.Close()
		require.NoError(t, err, "close mapped cnc file")
		_, err = cnc.ReadMetadataFile(md.CncPath())
		require.NoError(t, err, "read metadata after close")
		mappedAgain, err := cnc.MapCncFile(md.CncPath())
		require.NoError(t, err, "remap cnc file after close")
		defer func() {
			require.NoError(t, mappedAgain.Close(), "close remapped cnc file")
		}()
	})
}

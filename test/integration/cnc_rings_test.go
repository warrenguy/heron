package integration_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/ringbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestCncRingsLive(t *testing.T) {
	md := harness.Start(t)

	mapped, err := cnc.MapCncFile(md.CncPath())
	require.NoError(t, err)
	defer func() { _ = mapped.Close() }()

	rings, err := cnc.WireRings(mapped)
	require.NoError(t, err)
	require.NotNil(t, rings.ToDriver)
	require.NotNil(t, rings.ToClients)

	meta := rings.Meta
	require.Positive(t, meta.PID)

	wantDriverCapacity, err := ringbuffer.CheckCapacity(
		int(meta.ToDriverBufferLength),
		ringbuffer.MinCapacity,
	)
	require.NoError(t, err)
	require.Equal(t, wantDriverCapacity, rings.ToDriver.Capacity())

	wantClientsCapacity, err := broadcast.CheckCapacity(int(meta.ToClientsBufferLength))
	require.NoError(t, err)
	require.Equal(t, wantClientsCapacity, rings.ToClients.Capacity())

	require.Len(t, mapped.ToDriverBuffer(), int(meta.ToDriverBufferLength))
	require.Len(t, mapped.ToClientsBuffer(), int(meta.ToClientsBufferLength))

	require.Equal(t, rings.ToDriver.ProducerPosition(), rings.ToDriver.ConsumerPosition())
	require.False(t, rings.ToClients.ReceiveNext())
	require.Zero(t, rings.ToClients.LappedCount())
}

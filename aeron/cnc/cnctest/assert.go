package cnctest

import (
	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

type Reporter interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

func AssertMappedBufferLengths(t Reporter, mapped *cnc.MappedCncFile, meta cnc.Metadata) {
	t.Helper()

	regions := []struct {
		name   string
		length int32
		buf    []byte
	}{
		{"to-driver", meta.ToDriverBufferLength, mapped.ToDriverBuffer()},
		{"to-clients", meta.ToClientsBufferLength, mapped.ToClientsBuffer()},
		{
			"counters metadata",
			meta.CountersMetaDataBufferLength,
			mapped.CountersMetaDataBuffer(),
		},
		{
			"counters values",
			meta.CountersValuesBufferLength,
			mapped.CountersValuesBuffer(),
		},
		{"error log", meta.ErrorLogBufferLength, mapped.ErrorLogBuffer()},
	}
	for _, region := range regions {
		require.Len(t, region.buf, int(region.length), region.name)
	}
}

func AssertWiredRingsReady(t Reporter, mapped *cnc.MappedCncFile, rings *cnc.Rings) {
	t.Helper()

	meta := rings.Meta
	require.NotNil(t, rings.ToDriver)
	require.NotNil(t, rings.ToClients)

	wantDriverCapacity, err := ringbuffer.CheckCapacity(
		int(meta.ToDriverBufferLength),
		ringbuffer.MinCapacity,
	)
	require.NoError(t, err)
	require.Equal(t, wantDriverCapacity, rings.ToDriver.Capacity())

	wantClientsCapacity, err := broadcast.CheckCapacity(int(meta.ToClientsBufferLength))
	require.NoError(t, err)
	require.Equal(t, wantClientsCapacity, rings.ToClients.Capacity())

	AssertMappedBufferLengths(t, mapped, meta)

	require.Equal(t, rings.ToDriver.ProducerPosition(), rings.ToDriver.ConsumerPosition())
	require.False(t, rings.ToClients.ReceiveNext())
	require.Zero(t, rings.ToClients.LappedCount())
}

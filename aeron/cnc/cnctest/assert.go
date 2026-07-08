package cnctest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
	"github.com/warrenguy/heron/ringbuffer"
)

const (
	testToDriverBufferLength    int32 = 512
	testToClientsBufferLength   int32 = 256
	testErrorLogBufferLength    int32 = 32
	testClientLivenessTimeoutNs int64 = 5_000_000_000
	testStartTimestampMs        int64 = 1_700_000_000_000
	testPID                     int64 = 4242
	testFilePageSize            int32 = 4096
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

func accessorTestMetadata() cnc.Metadata {
	return cnc.Metadata{
		ToDriverBufferLength:         testToDriverBufferLength,
		ToClientsBufferLength:        testToClientsBufferLength,
		CountersMetaDataBufferLength: cnc.CounterMetadataLength,
		CountersValuesBufferLength:   cnc.CounterValueLength,
		ErrorLogBufferLength:         testErrorLogBufferLength,
	}
}

func writeMetadataFieldsToBuffer(buf []byte, meta cnc.Metadata) {
	atomicBuffer := atomicbuffer.Wrap(buf)
	atomicBuffer.PutInt32(cnc.OffsetToDriverBufferLength, meta.ToDriverBufferLength)
	atomicBuffer.PutInt32(cnc.OffsetToClientsBufferLength, meta.ToClientsBufferLength)
	atomicBuffer.PutInt32(
		cnc.OffsetCountersMetaDataBufferLength,
		meta.CountersMetaDataBufferLength,
	)
	atomicBuffer.PutInt32(cnc.OffsetCountersValuesBufferLength, meta.CountersValuesBufferLength)
	atomicBuffer.PutInt32(cnc.OffsetErrorLogBufferLength, meta.ErrorLogBufferLength)
	atomicBuffer.PutInt64(cnc.OffsetClientLivenessTimeout, testClientLivenessTimeoutNs)
	atomicBuffer.PutInt64(cnc.OffsetStartTimestamp, testStartTimestampMs)
	atomicBuffer.PutInt64(cnc.OffsetPID, testPID)
	atomicBuffer.PutInt32(cnc.OffsetFilePageSize, testFilePageSize)
	atomicBuffer.PutInt32Release(cnc.OffsetCncVersion, cnc.Version)
}

func OpenAccessorTestMappedCncFile(tb testing.TB) *cnc.MappedCncFile {
	tb.Helper()

	meta := accessorTestMetadata()
	path := filepath.Join(tb.TempDir(), cnc.FileName)
	buf := make([]byte, meta.ComputedLength())
	writeMetadataFieldsToBuffer(buf, meta)
	require.NoError(tb, os.WriteFile(path, buf, 0o644))

	mapped, err := cnc.MapCncFile(path)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = mapped.Close() })

	return mapped
}

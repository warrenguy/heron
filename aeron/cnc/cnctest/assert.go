// Package cnctest writes CnC files for tests. Its Assert helpers check mapped
// buffer lengths and wired rings.
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

// AssertWiredRingsConstruct asserts non-nil ring receivers, ring capacities,
// and mapped region lengths. Does not require empty ToDriver or ToClients.
//
// A connected shared driver may publish between drain and this assert.
func AssertWiredRingsConstruct(t Reporter, mapped *cnc.MappedCncFile, rings *cnc.Rings) {
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
}

// AssertWiredRingsReady asserts the same wiring as AssertWiredRingsConstruct,
// then asserts empty ToDriver and ToClients.
//
// Requires a quiescent ToDriver and ToClients for this receiver. Call only with
// synthetic CnC fixtures or an exclusive driver that has no concurrent traffic.
// Live shared-driver tests must use AssertWiredRingsConstruct instead.
func AssertWiredRingsReady(t Reporter, mapped *cnc.MappedCncFile, rings *cnc.Rings) {
	t.Helper()

	AssertWiredRingsConstruct(t, mapped, rings)

	require.Equal(t, rings.ToDriver.ProducerPosition(), rings.ToDriver.ConsumerPosition())
	require.False(t, rings.ToClients.ReceiveNext())
	require.Zero(t, rings.ToClients.LappedCount())
}

// CounterRecord is one counters-buffer record for a test CnC file.
type CounterRecord struct {
	CounterID      int32
	State          int32
	TypeID         int32
	RegistrationID int64
	Value          int64
}

func accessorTestMetadata(counterCount int) cnc.Metadata {
	return cnc.Metadata{
		ToDriverBufferLength:         testToDriverBufferLength,
		ToClientsBufferLength:        testToClientsBufferLength,
		CountersMetaDataBufferLength: int32(cnc.CounterMetadataLength * counterCount),
		CountersValuesBufferLength:   int32(cnc.CounterValueLength * counterCount),
		ErrorLogBufferLength:         testErrorLogBufferLength,
	}
}

// counterSlotCount returns buffer slots for records. Empty fixtures use one slot
// so a reader still has a scan range.
func counterSlotCount(records []CounterRecord) int {
	slots := 1
	for _, record := range records {
		slots = max(slots, int(record.CounterID)+1)
	}
	return slots
}

// writeMetadataFields writes the CnC header fields for meta into buf.
//
// Behaviour: writes the buffer lengths from meta and fixed test values for
// liveness timeout, start timestamp, PID, and page size. Releases the version
// field last so a reader sees a complete header.
//
// Ownership: the caller owns buf and sizes it to meta.ComputedLength.
func writeMetadataFields(buf []byte, meta cnc.Metadata) {
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

// WriteCncDir writes a CnC file holding records into a new temp directory and
// returns that directory.
//
// Behaviour: sizes the counters buffers to the highest record id.
//
// Ownership: tb.TempDir owns the returned directory.
func WriteCncDir(tb testing.TB, records []CounterRecord) string {
	tb.Helper()

	meta := accessorTestMetadata(counterSlotCount(records))
	buf := make([]byte, meta.ComputedLength())
	writeMetadataFields(buf, meta)

	countersMeta := buf[cnc.CountersMetaDataOffset(meta):cnc.CountersValuesOffset(meta)]
	countersValues := buf[cnc.CountersValuesOffset(meta):cnc.ErrorLogOffset(meta)]
	for _, record := range records {
		cnc.WriteCounterRecord(
			countersMeta,
			countersValues,
			record.CounterID,
			record.State,
			record.TypeID,
			record.RegistrationID,
			record.Value,
		)
	}

	dir := tb.TempDir()
	require.NoError(tb, os.WriteFile(filepath.Join(dir, cnc.FileName), buf, 0o644))
	return dir
}

func OpenAccessorTestMappedCncFile(tb testing.TB) *cnc.MappedCncFile {
	tb.Helper()

	path := filepath.Join(WriteCncDir(tb, nil), cnc.FileName)
	mapped, err := cnc.MapCncFile(path)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = mapped.Close() })

	return mapped
}

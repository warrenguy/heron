package cnc

import (
	"fmt"
	"math"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/agrona/mmap"
)

func WireRingsFromMapping(mapping *mmap.Mapping, meta Metadata) (*Rings, error) {
	if mapping == nil || mapping.Data == nil {
		return nil, fmt.Errorf("cnc: closed mapped file")
	}
	return wireRings(mapping, meta)
}

const (
	RingReadyToDriverBufferLength  int32 = 1049344
	RingReadyToClientsBufferLength int32 = 1048704

	TestToDriverBufferLength         int32 = 512
	TestToClientsBufferLength        int32 = 256
	TestCountersMetaDataBufferLength int32 = 128
	TestCountersValuesBufferLength   int32 = 64
	TestErrorLogBufferLength         int32 = 32

	TestClientLivenessTimeoutNs int64 = 5_000_000_000
	TestStartTimestampMs        int64 = 1_700_000_000_000
	TestPID                     int64 = 4242
	TestFilePageSize            int32 = 4096

	testShortFileTailLength         = 64
	testWrappedOverflowTailLength   = 72
	TestShortCncFileLength          = MetaDataLength + testShortFileTailLength
	TestWrappedOverflowOnDiskLength = MetaDataLength + testWrappedOverflowTailLength
	TestStaleFileSizeShrink         = testShortFileTailLength

	TestHugeRingBufferLength int32 = 1 << 30
	TestRapidMaxField              = 1 << 14
	TestRapidMaxFieldWide          = 1 << 16
	TestMaxCncFileLength           = 1 << 24
	TestLargeFileLength            = 1 << 20
)

func ReadyMetadata() Metadata {
	return Metadata{
		ToDriverBufferLength:         TestToDriverBufferLength,
		ToClientsBufferLength:        TestToClientsBufferLength,
		CountersMetaDataBufferLength: TestCountersMetaDataBufferLength,
		CountersValuesBufferLength:   TestCountersValuesBufferLength,
		ErrorLogBufferLength:         TestErrorLogBufferLength,
	}
}

func RingReadyMetadata() Metadata {
	meta := ReadyMetadata()
	meta.ToDriverBufferLength = RingReadyToDriverBufferLength
	meta.ToClientsBufferLength = RingReadyToClientsBufferLength
	return meta
}

func WrappedPositiveOverflowMetadata() Metadata {
	return Metadata{
		ToDriverBufferLength:         math.MaxInt32,
		ToClientsBufferLength:        math.MaxInt32,
		CountersMetaDataBufferLength: 1,
		CountersValuesBufferLength:   1,
		ErrorLogBufferLength:         testWrappedOverflowTailLength,
	}
}

func OverflowComputedLengthMetadata() Metadata {
	return Metadata{
		ToDriverBufferLength:  TestHugeRingBufferLength,
		ToClientsBufferLength: TestHugeRingBufferLength,
		ErrorLogBufferLength:  1,
	}
}

func writeReadyHeaderFields(atomicBuffer atomic.AtomicBuffer) {
	atomicBuffer.PutInt64(OffsetClientLivenessTimeout, TestClientLivenessTimeoutNs)
	atomicBuffer.PutInt64(OffsetStartTimestamp, TestStartTimestampMs)
	atomicBuffer.PutInt64(OffsetPID, TestPID)
	atomicBuffer.PutInt32(OffsetFilePageSize, TestFilePageSize)
	atomicBuffer.PutInt32Release(OffsetCncVersion, Version)
}

func WriteMetadataFieldsToBuffer(buf []byte, meta Metadata) {
	atomicBuffer := atomic.Wrap(buf)
	atomicBuffer.PutInt32(OffsetToDriverBufferLength, meta.ToDriverBufferLength)
	atomicBuffer.PutInt32(OffsetToClientsBufferLength, meta.ToClientsBufferLength)
	atomicBuffer.PutInt32(OffsetCountersMetaDataBufferLength, meta.CountersMetaDataBufferLength)
	atomicBuffer.PutInt32(OffsetCountersValuesBufferLength, meta.CountersValuesBufferLength)
	atomicBuffer.PutInt32(OffsetErrorLogBufferLength, meta.ErrorLogBufferLength)
	writeReadyHeaderFields(atomicBuffer)
}

func rapidDrawMax(minVal, maxSmall int) int {
	if minVal > maxSmall {
		return math.MaxInt32
	}
	return maxSmall
}

func drawRapidBufferLengths(
	t *rapid.T,
	maxSmall,
	driverMin,
	clientMin,
	errorLogMin int,
) Metadata {
	driverMax := rapidDrawMax(driverMin, maxSmall)
	clientMax := rapidDrawMax(clientMin, maxSmall)
	return Metadata{
		ToDriverBufferLength:         int32(rapid.IntRange(driverMin, driverMax).Draw(t, "toDriver")),
		ToClientsBufferLength:        int32(rapid.IntRange(clientMin, clientMax).Draw(t, "toClients")),
		CountersMetaDataBufferLength: int32(rapid.IntRange(0, maxSmall).Draw(t, "countersMetaData")),
		CountersValuesBufferLength:   int32(rapid.IntRange(0, maxSmall).Draw(t, "countersValues")),
		ErrorLogBufferLength:         int32(rapid.IntRange(errorLogMin, maxSmall).Draw(t, "errorLog")),
	}
}

func DrawRapidBufferLengths(t *rapid.T, maxField int) Metadata {
	return drawRapidBufferLengths(t, maxField, 1, 1, 0)
}

func DrawOverflowBufferLengths(t *rapid.T) Metadata {
	largeMin := int(TestHugeRingBufferLength)
	meta := drawRapidBufferLengths(t, TestRapidMaxFieldWide, largeMin, largeMin, 1)
	computedLength := meta.ComputedLength()
	if computedLength <= largeMin {
		t.Fatalf("draw did not produce huge computed length: %d", computedLength)
	}
	return meta
}

type assertReporter interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

func AssertMetadataBufferLengthsEqual(t assertReporter, got, want Metadata) {
	t.Helper()
	gotDescs := bufferLengthDescriptors(got)
	wantDescs := bufferLengthDescriptors(want)
	for i := range gotDescs {
		require.Equal(t, wantDescs[i].length, gotDescs[i].length, gotDescs[i].name)
	}
}

func AssertParsedReadyMetadataEqual(t assertReporter, got, want Metadata) {
	t.Helper()
	AssertMetadataBufferLengthsEqual(t, got, want)
	require.Equal(t, Version, got.CncVersion)
	require.Equal(t, TestClientLivenessTimeoutNs, got.ClientLivenessTimeoutNs)
	require.Equal(t, TestStartTimestampMs, got.StartTimestampMs)
	require.Equal(t, TestPID, got.PID)
	require.Equal(t, TestFilePageSize, got.FilePageSize)
}

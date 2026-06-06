package cnc

import (
	"math"

	"github.com/warrenguy/heron/aeron/atomic"
	"pgregory.net/rapid"
)

const (
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

type testFailer interface {
	Helper()
	Fatalf(format string, args ...any)
}

func AssertMetadataBufferLengthsEqual(t testFailer, got, want Metadata) {
	t.Helper()
	for _, check := range []struct {
		name string
		got  int32
		want int32
	}{
		{"to-driver", got.ToDriverBufferLength, want.ToDriverBufferLength},
		{"to-clients", got.ToClientsBufferLength, want.ToClientsBufferLength},
		{"counters metadata", got.CountersMetaDataBufferLength, want.CountersMetaDataBufferLength},
		{"counters values", got.CountersValuesBufferLength, want.CountersValuesBufferLength},
		{"error log", got.ErrorLogBufferLength, want.ErrorLogBufferLength},
	} {
		if check.got != check.want {
			t.Fatalf("%s %d want %d", check.name, check.got, check.want)
		}
	}
}

func AssertMappedBufferLengths(t testFailer, mapped *MappedCncFile, meta Metadata) {
	t.Helper()
	data := mapped.Bytes()
	if data == nil {
		t.Fatalf("mapped file closed or nil")
	}
	if err := validateMappedBufferLengths(meta, sliceCncRegions(data, meta)); err != nil {
		t.Fatalf("%v", err)
	}
}

func AssertParsedReadyMetadataEqual(t testFailer, got, want Metadata) {
	t.Helper()
	AssertMetadataBufferLengthsEqual(t, got, want)
	if got.CncVersion != Version {
		t.Fatalf("version %d want %d", got.CncVersion, Version)
	}
	if got.ClientLivenessTimeoutNs != TestClientLivenessTimeoutNs {
		t.Fatalf("client liveness timeout %d", got.ClientLivenessTimeoutNs)
	}
	if got.StartTimestampMs != TestStartTimestampMs {
		t.Fatalf("start timestamp %d", got.StartTimestampMs)
	}
	if got.PID != TestPID {
		t.Fatalf("pid %d", got.PID)
	}
	if got.FilePageSize != TestFilePageSize {
		t.Fatalf("file page size %d", got.FilePageSize)
	}
}

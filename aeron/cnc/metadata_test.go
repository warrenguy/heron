package cnc_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
)

const (
	testToDriverBufferLength         int32 = 512
	testToClientsBufferLength        int32 = 256
	testCountersMetaDataBufferLength int32 = 128
	testCountersValuesBufferLength   int32 = 64
	testErrorLogBufferLength         int32 = 32
)

func testReadyMetadata() cnc.Metadata {
	return cnc.Metadata{
		ToDriverBufferLength:         testToDriverBufferLength,
		ToClientsBufferLength:        testToClientsBufferLength,
		CountersMetaDataBufferLength: testCountersMetaDataBufferLength,
		CountersValuesBufferLength:   testCountersValuesBufferLength,
		ErrorLogBufferLength:         testErrorLogBufferLength,
	}
}

func testReadyFileLength() int {
	return testReadyMetadata().ComputedLength()
}

func writeReadyMetadata(t *testing.T, buf []byte) {
	t.Helper()
	atomicBuffer := atomic.Wrap(buf)
	atomicBuffer.PutInt32(cnc.OffsetToDriverBufferLength, testToDriverBufferLength)
	atomicBuffer.PutInt32(cnc.OffsetToClientsBufferLength, testToClientsBufferLength)
	atomicBuffer.PutInt32(cnc.OffsetCountersMetaDataBufferLength, testCountersMetaDataBufferLength)
	atomicBuffer.PutInt32(cnc.OffsetCountersValuesBufferLength, testCountersValuesBufferLength)
	atomicBuffer.PutInt32(cnc.OffsetErrorLogBufferLength, testErrorLogBufferLength)
	atomicBuffer.PutInt64(cnc.OffsetClientLivenessTimeout, 5_000_000_000)
	atomicBuffer.PutInt64(cnc.OffsetStartTimestamp, 1_700_000_000_000)
	atomicBuffer.PutInt64(cnc.OffsetPID, 4242)
	atomicBuffer.PutInt32(cnc.OffsetFilePageSize, 4096)
	atomicBuffer.PutInt32Release(cnc.OffsetCncVersion, cnc.Version)
}

func writeReadyCncFile(t *testing.T, path string) {
	t.Helper()
	buf := make([]byte, testReadyFileLength())
	writeReadyMetadata(t, buf)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestParseMetadataSmoke(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength+1024)
	writeReadyMetadata(t, buf)

	meta, err := cnc.ParseMetadata(buf)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CncVersion != cnc.Version {
		t.Fatalf("version %d", meta.CncVersion)
	}
	if meta.ToDriverBufferLength != testToDriverBufferLength {
		t.Fatalf("to-driver %d", meta.ToDriverBufferLength)
	}
	if meta.PID != 4242 {
		t.Fatalf("pid %d", meta.PID)
	}
	if meta.FilePageSize != 4096 {
		t.Fatalf("page size %d", meta.FilePageSize)
	}

	wantLen := testReadyFileLength()
	if meta.ComputedLength() != wantLen {
		t.Fatalf("computed %d want %d", meta.ComputedLength(), wantLen)
	}
	if !meta.IsFileLengthSufficient(wantLen) {
		t.Fatal("should be sufficient")
	}
	if meta.IsFileLengthSufficient(wantLen - 1) {
		t.Fatal("should be insufficient")
	}

	offsetTests := []struct {
		name string
		got  int
		want int
	}{
		{"to-driver", cnc.ToDriverOffset(), cnc.MetaDataLength},
		{
			"to-clients",
			cnc.ToClientsOffset(meta),
			cnc.ToDriverOffset() + int(meta.ToDriverBufferLength),
		},
		{
			"counters-meta",
			cnc.CountersMetaDataOffset(meta),
			cnc.ToClientsOffset(meta) + int(meta.ToClientsBufferLength),
		},
		{
			"counters-values",
			cnc.CountersValuesOffset(meta),
			cnc.CountersMetaDataOffset(meta) + int(meta.CountersMetaDataBufferLength),
		},
		{
			"error-log",
			cnc.ErrorLogOffset(meta),
			cnc.CountersValuesOffset(meta) + int(meta.CountersValuesBufferLength),
		},
	}
	for _, test := range offsetTests {
		if test.got != test.want {
			t.Fatalf("%s offset %d want %d", test.name, test.got, test.want)
		}
	}
}

func TestParseMetadataBufferTooShort(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength-1)
	_, err := cnc.ParseMetadata(buf)
	if err == nil {
		t.Fatal("expected buffer too short error")
	}
	if !strings.Contains(err.Error(), "buffer too short") {
		t.Fatalf("got %v", err)
	}
}

func TestParseMetadataNotReady(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength)
	_, err := cnc.ParseMetadata(buf)
	if !errors.Is(err, cnc.ErrNotReady) {
		t.Fatalf("got %v", err)
	}
}

func TestParseMetadataIncompatibleMajor(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength)
	writeReadyMetadata(t, buf)
	atomic.Wrap(buf).PutInt32Release(
		cnc.OffsetCncVersion,
		cnc.ComposeSemanticVersion(99, 0, 0),
	)

	_, err := cnc.ParseMetadata(buf)
	if err == nil {
		t.Fatal("expected incompatible major version error")
	}
	if !strings.Contains(err.Error(), "incompatible major version") {
		t.Fatalf("got %v", err)
	}
}

func writeMetadataNegativeDriverLength(t *testing.T, buf []byte) {
	t.Helper()
	writeReadyMetadata(t, buf)
	atomic.Wrap(buf).PutInt32(cnc.OffsetToDriverBufferLength, -1)
}

func TestParseMetadataNegativeLength(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength)
	writeMetadataNegativeDriverLength(t, buf)

	_, err := cnc.ParseMetadata(buf)
	if err == nil {
		t.Fatal("expected error for negative buffer length")
	}
}

func TestCncVersionVolatile(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength)
	atomicBuffer := atomic.Wrap(buf)
	atomicBuffer.PutInt32Release(cnc.OffsetCncVersion, cnc.Version)
	if got := cnc.CncVersionVolatile(buf); got != cnc.Version {
		t.Fatalf("volatile version %d", got)
	}
}

func TestCncVersionVolatile_bufferTooShort(t *testing.T) {
	if got := cnc.CncVersionVolatile([]byte{0, 0, 0}); got != 0 {
		t.Fatalf("got %d want 0", got)
	}
}

func TestComputeCncFileLength(t *testing.T) {
	ready := testReadyMetadata()
	total := ready.ComputedLength() - cnc.MetaDataLength
	want := atomic.Align(cnc.MetaDataLength+total, 4096)

	got, err := cnc.ComputeCncFileLength(total, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("length %d want %d", got, want)
	}

	if _, err := cnc.ComputeCncFileLength(total, 0); err == nil {
		t.Fatal("expected error for alignment 0")
	}
	if _, err := cnc.ComputeCncFileLength(total, 3); err == nil {
		t.Fatal("expected error for non-power-of-two alignment")
	}
}

func TestIsFileLengthSufficientNegativeLength(t *testing.T) {
	meta := cnc.Metadata{
		ToDriverBufferLength: -1,
	}
	if meta.IsFileLengthSufficient(1 << 20) {
		t.Fatal("negative length should not be sufficient")
	}
}

func TestReadMetadataFile_roundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	if _, err := cnc.ReadMetadataFile(path); err != nil {
		t.Fatal(err)
	}
}

func TestReadMetadataFile_notReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	buf := make([]byte, cnc.MetaDataLength+1)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := cnc.ReadMetadataFile(path)
	if !errors.Is(err, cnc.ErrNotReady) {
		t.Fatalf("got %v", err)
	}
}

func TestWaitCncReady_readyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	if err := cnc.WaitCncReady(path, time.Second); err != nil {
		t.Fatalf("ready file: %v", err)
	}
}

func TestWaitCncReady_insufficientFileLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	buf := make([]byte, testReadyFileLength())
	writeReadyMetadata(t, buf)

	const shortFile = cnc.MetaDataLength + 64
	if err := os.WriteFile(path, buf[:shortFile], 0o644); err != nil {
		t.Fatal(err)
	}

	err := cnc.WaitCncReady(path, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected insufficient file length error")
	}
	if !strings.Contains(err.Error(), "insufficient for metadata") {
		t.Fatalf("got %v", err)
	}
}

func TestWaitCncReady_missingFile(t *testing.T) {
	err := cnc.WaitCncReady(t.TempDir()+"/missing.dat", 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout")
	}
}

func TestWaitCncReady_invalidMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	buf := make([]byte, cnc.MetaDataLength+64)
	writeMetadataNegativeDriverLength(t, buf)

	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	err := cnc.WaitCncReady(path, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected invalid metadata error")
	}
	if !strings.Contains(err.Error(), "negative buffer length") {
		t.Fatalf("got %v", err)
	}
}

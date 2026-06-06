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

func TestParseMetadataSmoke(t *testing.T) {
	buf := make([]byte, cnc.MetaDataLength+1024)
	writeReadyMetadata(t, buf)

	meta, err := cnc.ParseMetadata(buf)
	if err != nil {
		t.Fatal(err)
	}
	cnc.AssertParsedReadyMetadataEqual(t, meta, cnc.ReadyMetadata())

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
	if !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("got %v", err)
	}
}

func TestParseMetadata_rejectsZeroRingBuffers(t *testing.T) {
	cases := []struct {
		name   string
		offset int
		field  string
	}{
		{"zero to-driver", cnc.OffsetToDriverBufferLength, "to-driver"},
		{"zero to-clients", cnc.OffsetToClientsBufferLength, "to-clients"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			buf := make([]byte, cnc.MetaDataLength)
			writeReadyMetadata(t, buf)
			atomic.Wrap(buf).PutInt32(test.offset, 0)

			_, err := cnc.ParseMetadata(buf)
			if err == nil {
				t.Fatalf("expected error for zero %s buffer length", test.field)
			}
			if !strings.Contains(err.Error(), "must be positive") {
				t.Fatalf("got %v", err)
			}
		})
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
	ready := cnc.ReadyMetadata()
	total := ready.ComputedLength() - cnc.MetaDataLength
	want := atomic.Align(cnc.MetaDataLength+total, int(cnc.TestFilePageSize))

	got, err := cnc.ComputeCncFileLength(total, int(cnc.TestFilePageSize))
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
	if meta.IsFileLengthSufficient(cnc.TestLargeFileLength) {
		t.Fatal("negative length should not be sufficient")
	}
}

func TestIsFileLengthSufficient_zeroRingBuffer(t *testing.T) {
	meta := cnc.Metadata{
		ToDriverBufferLength: 512,
	}
	if meta.IsFileLengthSufficient(cnc.TestLargeFileLength) {
		t.Fatal("zero to-clients length should not be sufficient")
	}
}

func TestReadMetadataFile_roundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	if _, err := cnc.ReadMetadataFile(path); err != nil {
		t.Fatal(err)
	}
}

func TestReadMetadataFile_hugeMetadataShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	fileLength := writeWrappedPositiveOverflowCncFile(t, path)
	meta := cnc.WrappedPositiveOverflowMetadata()
	if meta.ComputedLength() <= fileLength {
		t.Fatalf("computed %d want > file %d", meta.ComputedLength(), fileLength)
	}

	_, err := cnc.ReadMetadataFile(path)
	if err == nil {
		t.Fatal("expected insufficient file length error")
	}
	if !strings.Contains(err.Error(), "insufficient for metadata") {
		t.Fatalf("got %v", err)
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

func TestWaitCncReady_hugeMetadataShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeWrappedPositiveOverflowCncFile(t, path)

	err := cnc.WaitCncReady(path, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected insufficient file length error")
	}
	if !strings.Contains(err.Error(), "insufficient for metadata") {
		t.Fatalf("got %v", err)
	}
}

func TestWaitCncReady_insufficientFileLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	buf := make([]byte, testReadyFileLength())
	writeReadyMetadata(t, buf)

	shortFile := cnc.TestShortCncFileLength
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
	buf := make([]byte, cnc.TestShortCncFileLength)
	writeMetadataNegativeDriverLength(t, buf)

	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	err := cnc.WaitCncReady(path, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected invalid metadata error")
	}
	if !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("got %v", err)
	}
}

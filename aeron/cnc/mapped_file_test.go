package cnc_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
)

func openFDCount(t *testing.T) int {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("open FD count requires linux /proc")
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestMapCncFile_readySynthetic(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	mapped, err := cnc.MapCncFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mapped.Close() }()

	cnc.AssertMappedBufferLengths(t, mapped, mapped.Metadata())
}

func TestMapCncFile_notReady(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"version zero", make([]byte, cnc.MetaDataLength+1)},
		{"short header", make([]byte, 64)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), cnc.FileName)
			if err := os.WriteFile(path, test.data, 0o644); err != nil {
				t.Fatal(err)
			}

			_, err := cnc.MapCncFile(path)
			if !errors.Is(err, cnc.ErrNotReady) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestMapCncFile_insufficientFileLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	buf := make([]byte, testReadyFileLength())
	writeReadyMetadata(t, buf)

	shortFile := cnc.TestShortCncFileLength
	const sentinel = 0xAB
	buf[shortFile-1] = sentinel
	if err := os.WriteFile(path, buf[:shortFile], 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := cnc.MapCncFile(path)
	if err == nil {
		t.Fatal("expected insufficient file length error")
	}
	wantNeed := cnc.ReadyMetadata().ComputedLength()
	wantErr := fmt.Sprintf(
		"cnc: file length %d insufficient for metadata (need %d)",
		shortFile,
		wantNeed,
	)
	if err.Error() != wantErr {
		t.Fatalf("got %v want %q", err, wantErr)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != shortFile {
		t.Fatalf("file size %d want %d", len(raw), shortFile)
	}
	if raw[shortFile-1] != sentinel {
		t.Fatalf("file mutated before mmap: sentinel %x", raw[shortFile-1])
	}
}

func assertMapCncRejectsInsufficientFile(t *testing.T, path string) {
	t.Helper()
	before := openFDCount(t)
	_, err := cnc.MapCncFile(path)
	after := openFDCount(t)
	if err == nil {
		t.Fatal("expected insufficient file length error")
	}
	if !strings.Contains(err.Error(), "insufficient for metadata") {
		t.Fatalf("got %v", err)
	}
	if after != before {
		t.Fatalf("FD count after %d want %d", after, before)
	}
}

func TestMapCncFile_hugeMetadataShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	fileLength := writeWrappedPositiveOverflowCncFile(t, path)
	meta := cnc.WrappedPositiveOverflowMetadata()
	if meta.ComputedLength() <= fileLength {
		t.Fatalf("computed %d want > file %d", meta.ComputedLength(), fileLength)
	}

	assertMapCncRejectsInsufficientFile(t, path)
}

func TestMapCncFile_overflowMetadataShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	meta := cnc.OverflowComputedLengthMetadata()
	writeOverflowCncFile(t, path, meta)

	assertMapCncRejectsInsufficientFile(t, path)
}

func TestMapCncFile_missingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.dat")

	_, err := cnc.MapCncFile(path)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

func TestMapCncFile_closeIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	mapped, err := cnc.MapCncFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := mapped.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mapped.Close(); err != nil {
		t.Fatal(err)
	}
	if mapped.ToDriverBuffer() != nil {
		t.Fatal("expected nil to-driver buffer after close")
	}
	if mapped.ToDriverAtomic().Capacity() != 0 {
		t.Fatal("expected zero to-driver atomic capacity after close")
	}
}

func TestMappedCncFile_atomicViews(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	mapped, err := cnc.MapCncFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mapped.Close() }()

	toDriver := mapped.ToDriverBuffer()
	toDriverAtomic := mapped.ToDriverAtomic()
	if toDriverAtomic.Capacity() != len(toDriver) {
		t.Fatalf("to-driver atomic capacity %d want %d", toDriverAtomic.Capacity(), len(toDriver))
	}
	toDriverAtomic.PutInt32(0, 42)
	if atomic.Wrap(toDriver).GetInt32(0) != 42 {
		t.Fatal("to-driver atomic write not visible in byte slice")
	}

	toClients := mapped.ToClientsBuffer()
	toClientsAtomic := mapped.ToClientsAtomic()
	if toClientsAtomic.Capacity() != len(toClients) {
		t.Fatalf("to-clients atomic capacity %d want %d", toClientsAtomic.Capacity(), len(toClients))
	}
	toClientsAtomic.PutInt32(0, 99)
	if atomic.Wrap(toClients).GetInt32(0) != 99 {
		t.Fatal("to-clients atomic write not visible in byte slice")
	}
}

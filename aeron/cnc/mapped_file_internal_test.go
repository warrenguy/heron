package cnc

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMapCncFile_restatRejectsTruncatedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	meta := writeReadyCncFileAt(t, path)
	computedLength := meta.ComputedLength()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	headerMeta, err := readMetadataHeaderFromFile(file, path, int(fileInfo.Size()), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Truncate(path, int64(computedLength-1)); err != nil {
		t.Fatal(err)
	}

	_, err = mapCncFileAfterHeader(file, path, headerMeta)
	if err == nil {
		t.Fatal("expected insufficient file length after truncate")
	}
	want := "cnc: file length"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v want substring %q", err, want)
	}
}

func TestReadMetadataHeaderFromFile_staleFileSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	meta := writeReadyCncFileAt(t, path)
	computedLength := meta.ComputedLength()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	staleSize := computedLength - TestStaleFileSizeShrink
	_, err = readMetadataHeaderFromFile(file, path, staleSize, nil)
	if err == nil {
		t.Fatal("expected insufficient file length for stale fileSize")
	}
	if !strings.Contains(err.Error(), "insufficient for metadata") {
		t.Fatalf("got %v", err)
	}
}

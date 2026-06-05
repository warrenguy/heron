package mmap_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/warrenguy/heron/aeron/mmap"
)

func TestMap_nonPositiveLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "map.bin")

	if _, err := mmap.Map(path, 0); err == nil {
		t.Fatal("expected error for length 0")
	}
	if _, err := mmap.Map(path, -1); err == nil {
		t.Fatal("expected error for negative length")
	}
}

func TestPreTouch_zeroPageSize(t *testing.T) {
	data := make([]byte, 128)
	mmap.PreTouch(data, 0)
}

func TestClose_nilMapping(t *testing.T) {
	var mapping *mmap.Mapping
	if err := mapping.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMapRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "map.bin")
	const length = 4096

	m, err := mmap.Map(path, length)
	if err != nil {
		t.Fatal(err)
	}
	mmap.PreTouch(m.Data, 4096)
	m.Data[0] = 0xAB
	m.Data[length-1] = 0xCD
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := mmap.Map(path, length)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m2.Close() }()

	if m2.Data[0] != 0xAB || m2.Data[length-1] != 0xCD {
		t.Fatalf("persisted bytes mismatch: %x %x", m2.Data[0], m2.Data[length-1])
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != length {
		t.Fatalf("file size %d want %d", len(raw), length)
	}
}

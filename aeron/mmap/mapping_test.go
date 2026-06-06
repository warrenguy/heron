package mmap_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/warrenguy/heron/aeron/mmap"
)

func testMapPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

func TestMap_nonPositiveLength(t *testing.T) {
	path := testMapPath(t, "map.bin")

	cases := []struct {
		name  string
		mapFn func(string, int) (*mmap.Mapping, error)
	}{
		{name: "MapCreate", mapFn: mmap.MapCreate},
		{name: "MapExisting", mapFn: mmap.MapExisting},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.mapFn(path, 0)
			if err == nil {
				t.Fatal("expected error for length 0")
			}
			if err.Error() != "mmap: length must be positive" {
				t.Fatalf("unexpected error: %v", err)
			}
			_, err = tc.mapFn(path, -1)
			if err == nil {
				t.Fatal("expected error for negative length")
			}
		})
	}
}

func TestClose_nilMapping(t *testing.T) {
	var mapping *mmap.Mapping
	if err := mapping.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMapCreate_roundtrip(t *testing.T) {
	path := testMapPath(t, "map.bin")
	const length = 4096

	m, err := mmap.MapCreate(path, length)
	if err != nil {
		t.Fatal(err)
	}
	mmap.PreTouch(m.Data, 0)
	m.Data[0] = 0xAB
	m.Data[length-1] = 0xCD
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := mmap.MapCreate(path, length)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m2.Close() }()

	if m2.Data[0] != 0xAB || m2.Data[length-1] != 0xCD {
		t.Fatalf("persisted bytes mismatch: %x %x", m2.Data[0], m2.Data[length-1])
	}
}

func TestMapOpenedFile_nonPositiveLengthCallerCloses(t *testing.T) {
	path := testMapPath(t, "opened.bin")
	if err := os.WriteFile(path, make([]byte, 64), 0o644); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = mmap.MapOpenedFile(file, 0)
	if err == nil {
		t.Fatal("expected error for length 0")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMapOpenedFile_roundtrip(t *testing.T) {
	path := testMapPath(t, "opened.bin")
	const length = 4096

	if err := os.WriteFile(path, make([]byte, length), 0o644); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	mapping, err := mmap.MapOpenedFile(file, length)
	if err != nil {
		t.Fatal(err)
	}
	mapping.Data[0] = 0xBE
	if err := mapping.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != 0xBE {
		t.Fatalf("persisted byte %x want 0xBE", raw[0])
	}
}

func TestMapExisting_missingFile(t *testing.T) {
	path := testMapPath(t, "missing.bin")

	_, err := mmap.MapExisting(path, 4096)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

func TestMapExisting_fileTooShort(t *testing.T) {
	path := testMapPath(t, "short.bin")
	const (
		fileLength = 1024
		mapLength  = 4096
	)

	if err := os.WriteFile(path, make([]byte, fileLength), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := mmap.MapExisting(path, mapLength)
	if err == nil {
		t.Fatal("expected error for file too short")
	}
	if err.Error() != "mmap: file too short: 1024 < 4096" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMapExisting_partialMap(t *testing.T) {
	t.Run("close_without_write", func(t *testing.T) {
		path := testMapPath(t, "existing.bin")
		const length = 4096

		initial := make([]byte, length)
		initial[0] = 0x11
		initial[length-1] = 0x22
		if err := os.WriteFile(path, initial, 0o644); err != nil {
			t.Fatal(err)
		}

		m, err := mmap.MapExisting(path, length)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) != length {
			t.Fatalf("file size %d want %d", len(raw), length)
		}
		if raw[0] != 0x11 || raw[length-1] != 0x22 {
			t.Fatalf("content changed: %x %x", raw[0], raw[length-1])
		}
	})

	t.Run("partial_prefix", func(t *testing.T) {
		path := testMapPath(t, "large.bin")
		const (
			fileLength = 8192
			mapLength  = 4096
		)

		initial := make([]byte, fileLength)
		initial[0] = 0xAA
		initial[mapLength-1] = 0xBB
		initial[mapLength] = 0xCC
		if err := os.WriteFile(path, initial, 0o644); err != nil {
			t.Fatal(err)
		}

		m, err := mmap.MapExisting(path, mapLength)
		if err != nil {
			t.Fatal(err)
		}

		if len(m.Data) != mapLength {
			t.Fatalf("mapped length %d want %d", len(m.Data), mapLength)
		}
		if m.Data[0] != 0xAA || m.Data[mapLength-1] != 0xBB {
			t.Fatalf("mapped bytes mismatch: %x %x", m.Data[0], m.Data[mapLength-1])
		}
		m.Data[0] = 0x55
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) != fileLength {
			t.Fatalf("file size %d want %d", len(raw), fileLength)
		}
		if raw[0] != 0x55 {
			t.Fatalf("mapped write not persisted: %x want 0x55", raw[0])
		}
		if raw[mapLength] != 0xCC {
			t.Fatalf("byte beyond map region %x want 0xCC", raw[mapLength])
		}
	})

	t.Run("full_read_write", func(t *testing.T) {
		path := testMapPath(t, "rw.bin")
		const length = 4096

		if err := os.WriteFile(path, make([]byte, length), 0o644); err != nil {
			t.Fatal(err)
		}

		m, err := mmap.MapExisting(path, length)
		if err != nil {
			t.Fatal(err)
		}
		m.Data[0] = 0xDE
		m.Data[length-1] = 0xAD
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if raw[0] != 0xDE || raw[length-1] != 0xAD {
			t.Fatalf("persisted bytes mismatch: %x %x", raw[0], raw[length-1])
		}
	})
}

func TestMapCreateWouldDestroyLiveFile(t *testing.T) {
	path := testMapPath(t, "live.bin")
	const (
		liveLength   = 8192
		createLength = 4096
	)

	initial := make([]byte, liveLength)
	initial[0] = 0xFE
	initial[createLength-1] = 0xED
	initial[createLength] = 0xCA
	initial[liveLength-1] = 0xFE
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := mmap.MapCreate(path, createLength)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != createLength {
		t.Fatalf("file size %d want %d", len(raw), createLength)
	}
	if raw[createLength-1] != 0xED {
		t.Fatalf("truncated tail byte %x want 0xED", raw[createLength-1])
	}
}

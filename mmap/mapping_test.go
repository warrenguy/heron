package mmap_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/mmap"
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
			for _, length := range []int{0, -1} {
				_, err := tc.mapFn(path, length)
				require.EqualError(t, err, "mmap: length must be positive")
			}
		})
	}
}

func TestClose_nilMapping(t *testing.T) {
	var mapping *mmap.Mapping
	require.NoError(t, mapping.Close())
}

func TestMapCreate_roundtrip(t *testing.T) {
	path := testMapPath(t, "map.bin")
	const length = 4096

	m, err := mmap.MapCreate(path, length)
	require.NoError(t, err)
	mmap.PreTouch(m.Data, 0)
	m.Data[0] = 0xAB
	m.Data[length-1] = 0xCD
	require.NoError(t, m.Close())

	m2, err := mmap.MapCreate(path, length)
	require.NoError(t, err)
	defer func() { _ = m2.Close() }()

	require.Equal(t, byte(0xAB), m2.Data[0])
	require.Equal(t, byte(0xCD), m2.Data[length-1])
}

func TestMapOpenedFile_nonPositiveLengthCallerCloses(t *testing.T) {
	path := testMapPath(t, "opened.bin")
	require.NoError(t, os.WriteFile(path, make([]byte, 64), 0o644))

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = mmap.MapOpenedFile(file, 0)
	require.EqualError(t, err, "mmap: length must be positive")
	require.NoError(t, file.Close())
}

func TestMapOpenedFile_roundtrip(t *testing.T) {
	path := testMapPath(t, "opened.bin")
	const length = 4096

	require.NoError(t, os.WriteFile(path, make([]byte, length), 0o644))

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	mapping, err := mmap.MapOpenedFile(file, length)
	require.NoError(t, err)
	mapping.Data[0] = 0xBE
	require.NoError(t, mapping.Close())

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte(0xBE), raw[0])
}

func TestMapExisting_missingFile(t *testing.T) {
	path := testMapPath(t, "missing.bin")

	_, err := mmap.MapExisting(path, 4096)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMapExisting_fileTooShort(t *testing.T) {
	path := testMapPath(t, "short.bin")
	const (
		fileLength = 1024
		mapLength  = 4096
	)

	require.NoError(t, os.WriteFile(path, make([]byte, fileLength), 0o644))

	_, err := mmap.MapExisting(path, mapLength)
	require.EqualError(t, err, "mmap: file too short: 1024 < 4096")
}

func TestMapExisting_partialMap(t *testing.T) {
	t.Run("close_without_write", func(t *testing.T) {
		path := testMapPath(t, "existing.bin")
		const length = 4096

		initial := make([]byte, length)
		initial[0] = 0x11
		initial[length-1] = 0x22
		require.NoError(t, os.WriteFile(path, initial, 0o644))

		m, err := mmap.MapExisting(path, length)
		require.NoError(t, err)
		require.NoError(t, m.Close())

		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Len(t, raw, length)
		require.Equal(t, byte(0x11), raw[0])
		require.Equal(t, byte(0x22), raw[length-1])
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
		require.NoError(t, os.WriteFile(path, initial, 0o644))

		m, err := mmap.MapExisting(path, mapLength)
		require.NoError(t, err)

		require.Len(t, m.Data, mapLength)
		require.Equal(t, byte(0xAA), m.Data[0])
		require.Equal(t, byte(0xBB), m.Data[mapLength-1])
		m.Data[0] = 0x55
		require.NoError(t, m.Close())

		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Len(t, raw, fileLength)
		require.Equal(t, byte(0x55), raw[0])
		require.Equal(t, byte(0xCC), raw[mapLength])
	})

	t.Run("full_read_write", func(t *testing.T) {
		path := testMapPath(t, "rw.bin")
		const length = 4096

		require.NoError(t, os.WriteFile(path, make([]byte, length), 0o644))

		m, err := mmap.MapExisting(path, length)
		require.NoError(t, err)
		m.Data[0] = 0xDE
		m.Data[length-1] = 0xAD
		require.NoError(t, m.Close())

		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, byte(0xDE), raw[0])
		require.Equal(t, byte(0xAD), raw[length-1])
	})
}

func TestMapping_Sync_roundtrip(t *testing.T) {
	path := testMapPath(t, "sync.bin")
	const length = 4096

	m, err := mmap.MapCreate(path, length)
	require.NoError(t, err)
	m.Data[0] = 0x42
	require.NoError(t, m.Sync())
	require.NoError(t, m.Close())

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte(0x42), raw[0])
}

func TestMapping_Sync_nilMapping(t *testing.T) {
	var mapping *mmap.Mapping
	require.NoError(t, mapping.Sync())
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
	require.NoError(t, os.WriteFile(path, initial, 0o644))

	m, err := mmap.MapCreate(path, createLength)
	require.NoError(t, err)
	require.NoError(t, m.Close())

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, raw, createLength)
	require.Equal(t, byte(0xED), raw[createLength-1])
}

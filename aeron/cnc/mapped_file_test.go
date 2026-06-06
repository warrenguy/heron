package cnc_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
)

func openFDCount(t *testing.T) int {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("open FD count requires linux /proc")
	}
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

func TestMapCncFile_readySynthetic(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	mapped, err := cnc.MapCncFile(path)
	require.NoError(t, err)
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
			require.NoError(t, os.WriteFile(path, test.data, 0o644))

			_, err := cnc.MapCncFile(path)
			require.ErrorIs(t, err, cnc.ErrNotReady)
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
	require.NoError(t, os.WriteFile(path, buf[:shortFile], 0o644))

	_, err := cnc.MapCncFile(path)
	require.Error(t, err)
	wantNeed := cnc.ReadyMetadata().ComputedLength()
	wantErr := fmt.Sprintf(
		"cnc: file length %d insufficient for metadata (need %d)",
		shortFile,
		wantNeed,
	)
	require.EqualError(t, err, wantErr)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, raw, shortFile)
	require.Equal(t, byte(sentinel), raw[shortFile-1])
}

func assertMapCncRejectsInsufficientFile(t *testing.T, path string) {
	t.Helper()
	before := openFDCount(t)
	_, err := cnc.MapCncFile(path)
	after := openFDCount(t)
	require.ErrorContains(t, err, "insufficient for metadata")
	require.Equal(t, before, after)
}

func TestMapCncFile_hugeMetadataShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	fileLength := writeWrappedPositiveOverflowCncFile(t, path)
	meta := cnc.WrappedPositiveOverflowMetadata()
	require.Greater(t, meta.ComputedLength(), fileLength)

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
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMapCncFile_closeIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	mapped, err := cnc.MapCncFile(path)
	require.NoError(t, err)
	require.NoError(t, mapped.Close())
	require.NoError(t, mapped.Close())
	require.Nil(t, mapped.ToDriverBuffer())
	require.Equal(t, 0, mapped.ToDriverAtomic().Capacity())
}

func TestMappedCncFile_atomicViews(t *testing.T) {
	path := filepath.Join(t.TempDir(), cnc.FileName)
	writeReadyCncFile(t, path)

	mapped, err := cnc.MapCncFile(path)
	require.NoError(t, err)
	defer func() { _ = mapped.Close() }()

	toDriver := mapped.ToDriverBuffer()
	toDriverAtomic := mapped.ToDriverAtomic()
	require.Equal(t, len(toDriver), toDriverAtomic.Capacity())
	toDriverAtomic.PutInt32(0, 42)
	require.Equal(t, int32(42), atomic.Wrap(toDriver).GetInt32(0))

	toClients := mapped.ToClientsBuffer()
	toClientsAtomic := mapped.ToClientsAtomic()
	require.Equal(t, len(toClients), toClientsAtomic.Capacity())
	toClientsAtomic.PutInt32(0, 99)
	require.Equal(t, int32(99), atomic.Wrap(toClients).GetInt32(0))
}

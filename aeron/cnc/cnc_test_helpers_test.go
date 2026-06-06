package cnc_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/cnc"
)

func testReadyFileLength() int {
	return cnc.ReadyMetadata().ComputedLength()
}

func writeMetadataToBuffer(t testing.TB, buf []byte, meta cnc.Metadata) {
	t.Helper()
	cnc.WriteMetadataFieldsToBuffer(buf, meta)
}

func writeReadyMetadata(t testing.TB, buf []byte) {
	t.Helper()
	writeMetadataToBuffer(t, buf, cnc.ReadyMetadata())
}

func writeCncFile(t testing.TB, path string, meta cnc.Metadata) {
	t.Helper()
	computed := meta.ComputedLength()
	require.GreaterOrEqual(
		t,
		computed,
		0,
		"computed length %d invalid",
		computed,
	)
	require.GreaterOrEqual(
		t,
		computed,
		cnc.MetaDataLength,
		"computed length %d < metadata header %d",
		computed,
		cnc.MetaDataLength,
	)
	require.LessOrEqual(
		t,
		computed,
		cnc.TestMaxCncFileLength,
		"computed length %d exceeds test cap %d; use writeOverflowCncFile for huge layouts",
		computed,
		cnc.TestMaxCncFileLength,
	)
	buf := make([]byte, computed)
	writeMetadataToBuffer(t, buf, meta)
	require.NoError(t, os.WriteFile(path, buf, 0o644))
}

func writeReadyCncFile(t testing.TB, path string) {
	t.Helper()
	writeCncFile(t, path, cnc.ReadyMetadata())
}

func readyCncFilePath(tb testing.TB) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), cnc.FileName)
	writeReadyCncFile(tb, path)
	return path
}

func openReadyMappedCncFile(tb testing.TB) *cnc.MappedCncFile {
	tb.Helper()
	path := readyCncFilePath(tb)
	mapped, err := cnc.MapCncFile(path)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = mapped.Close() })
	return mapped
}

func writeWrappedPositiveOverflowCncFile(t testing.TB, path string) int {
	t.Helper()
	fileLength := cnc.TestWrappedOverflowOnDiskLength
	buf := make([]byte, fileLength)
	writeMetadataToBuffer(t, buf, cnc.WrappedPositiveOverflowMetadata())
	require.NoError(t, os.WriteFile(path, buf, 0o644))
	return fileLength
}

// writeOverflowCncFile is for huge-metadata / short-on-disk tests only. It writes a file
// shorter than meta.ComputedLength() with valid metadata header fields.
func writeOverflowCncFile(tb testing.TB, path string, meta cnc.Metadata) {
	tb.Helper()
	shortFile := cnc.TestShortCncFileLength
	computed := meta.ComputedLength()
	require.GreaterOrEqual(
		tb,
		computed,
		0,
		"computed length %d invalid",
		computed,
	)
	require.Greater(
		tb,
		computed,
		shortFile,
		"computed length %d must exceed short file %d for overflow test",
		computed,
		shortFile,
	)
	buf := make([]byte, shortFile)
	writeMetadataToBuffer(tb, buf, meta)
	require.NoError(tb, os.WriteFile(path, buf, 0o644))
}

package cnc_test

import (
	"os"
	"testing"

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
	if computed < 0 {
		t.Fatalf("computed length %d invalid", computed)
	}
	if computed < cnc.MetaDataLength {
		t.Fatalf(
			"computed length %d < metadata header %d",
			computed,
			cnc.MetaDataLength,
		)
	}
	if computed > cnc.TestMaxCncFileLength {
		t.Fatalf(
			"computed length %d exceeds test cap %d; use writeOverflowCncFile for huge layouts",
			computed,
			cnc.TestMaxCncFileLength,
		)
	}
	buf := make([]byte, computed)
	writeMetadataToBuffer(t, buf, meta)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeReadyCncFile(t testing.TB, path string) {
	t.Helper()
	writeCncFile(t, path, cnc.ReadyMetadata())
}

func writeWrappedPositiveOverflowCncFile(t testing.TB, path string) int {
	t.Helper()
	fileLength := cnc.TestWrappedOverflowOnDiskLength
	buf := make([]byte, fileLength)
	writeMetadataToBuffer(t, buf, cnc.WrappedPositiveOverflowMetadata())
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	return fileLength
}

// writeOverflowCncFile is for huge-metadata / short-on-disk tests only. It writes a file
// shorter than meta.ComputedLength() with valid metadata header fields.
func writeOverflowCncFile(tb testing.TB, path string, meta cnc.Metadata) {
	tb.Helper()
	shortFile := cnc.TestShortCncFileLength
	computed := meta.ComputedLength()
	if computed < 0 {
		tb.Fatalf("computed length %d invalid", computed)
	}
	if computed <= shortFile {
		tb.Fatalf(
			"computed length %d must exceed short file %d for overflow test",
			computed,
			shortFile,
		)
	}
	buf := make([]byte, shortFile)
	writeMetadataToBuffer(tb, buf, meta)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		tb.Fatal(err)
	}
}

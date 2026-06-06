package cnc_test

import (
	"path/filepath"
	"testing"

	"github.com/warrenguy/heron/aeron/cnc"
)

func writeBenchmarkCncPath(b *testing.B) string {
	b.Helper()
	path := filepath.Join(b.TempDir(), cnc.FileName)
	writeReadyCncFile(b, path)
	return path
}

func benchmarkReadyMappedCncFile(b *testing.B) *cnc.MappedCncFile {
	b.Helper()
	mapped, err := cnc.MapCncFile(writeBenchmarkCncPath(b))
	if err != nil {
		b.Fatal(err)
	}
	return mapped
}

func BenchmarkMapCncFile(b *testing.B) {
	path := writeBenchmarkCncPath(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mapped, err := cnc.MapCncFile(path)
		if err != nil {
			b.Fatal(err)
		}
		if err := mapped.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMappedCncFile_ToDriverAtomic(b *testing.B) {
	mapped := benchmarkReadyMappedCncFile(b)
	defer func() { _ = mapped.Close() }()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = mapped.ToDriverAtomic()
	}
}

func BenchmarkMappedCncFile_ToDriverBuffer(b *testing.B) {
	mapped := benchmarkReadyMappedCncFile(b)
	defer func() { _ = mapped.Close() }()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = mapped.ToDriverBuffer()
	}
}

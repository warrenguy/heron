package cnc_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/cnc"
)

func writeBenchmarkCncPath(b *testing.B) string {
	b.Helper()
	return readyCncFilePath(b)
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
	mapped := openReadyMappedCncFile(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = mapped.ToDriverAtomic()
	}
}

func BenchmarkMappedCncFile_ToDriverBuffer(b *testing.B) {
	mapped := openReadyMappedCncFile(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = mapped.ToDriverBuffer()
	}
}

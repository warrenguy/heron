package sbe_test

import (
	"encoding/binary"
	"testing"

	"github.com/warrenguy/heron/aeron/sbe"
)

func BenchmarkReadVarSlice(b *testing.B) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[2:], 4)
	copy(buf[6:], []byte("data"))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _, _ = sbe.ReadVarSlice(buf, 2, 8)
	}
}

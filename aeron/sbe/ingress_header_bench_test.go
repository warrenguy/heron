package sbe_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/sbe"
)

func BenchmarkEncodeIngressHeader(b *testing.B) {
	buf, fields := ingressEncodeFixture(0)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = sbe.EncodeIngressHeader(buf, 0, fields)
	}
}

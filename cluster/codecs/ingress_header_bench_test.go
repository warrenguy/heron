package codecs_test

import (
	"testing"

	"luas.fast/cluster/codecs"
)

func BenchmarkEncodeIngressHeader(b *testing.B) {
	buf, fields := ingressEncodeFixture(0)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = codecs.EncodeIngressHeader(buf, 0, fields)
	}
}

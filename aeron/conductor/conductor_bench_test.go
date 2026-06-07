package conductor_test

import (
	"testing"
	"time"
)

func BenchmarkConductor_DoWork(b *testing.B) {
	conductorInstance := newConductorForTest(
		b,
		conductorTestConfig(b, func(error) {}, fixedClocks{nanoNs: 0}),
	)
	conductorInstance.DoWork()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		conductorInstance.DoWork()
	}
}

func BenchmarkConductor_DoWorkKeepaliveTick(b *testing.B) {
	var nanoNs int64
	epochMs := int64(1_700_000_000_123)
	nanoNs = (600 * time.Millisecond).Nanoseconds()

	conductorInstance := newConductorForTest(
		b,
		conductorTestConfigMutableNano(b, func(error) {}, epochMs, &nanoNs),
		withHeartbeatCounter(b, conductorTestClientID, 0),
	)

	nanoNs = (600 * time.Millisecond).Nanoseconds()
	conductorInstance.DoWork()

	tickStep := conductorTestKeepAliveInterval.Nanoseconds() + 1

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		nanoNs += tickStep
		conductorInstance.DoWork()
	}
}

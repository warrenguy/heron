package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func setupBenchExclusivePublication(b *testing.B) *aeron.ExclusivePublication {
	b.Helper()
	ep := aeron.NewTestExclusivePublication(
		b,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		int64(1)<<62,
	)
	benchOfferSink = ep.Offer(benchPayload64[:])
	return ep
}

func BenchmarkExclusivePublication_Offer_Small(b *testing.B) {
	ep := setupBenchExclusivePublication(b)
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		benchOfferSink = aeron.OfferRetryForTest(ep, benchPayload64[:])
	}
}

func BenchmarkExclusivePublication_TryClaim_Commit(b *testing.B) {
	ep := setupBenchExclusivePublication(b)
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(ep, 64)
		if err != nil {
			b.Fatal(err)
		}
		if position < 0 {
			b.Fatal("tryClaim failed")
		}
		termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
		termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, benchPayload64[:])
		if err := ep.Commit(termIndex, frameOffset); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExclusivePublication_Offer_TermBoundary(b *testing.B) {
	cfg := aeron.LogConfigNearTermEndForTest(aeron.DefaultTestExclusivePublicationLogConfig, 128)
	ep := aeron.NewTestExclusivePublication(b, cfg, int64(1)<<62)
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		benchOfferSink = aeron.OfferRetryForTest(ep, benchPayload64[:])
	}
}

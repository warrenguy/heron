package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var (
	benchOfferSink   int64
	benchPayload64   = [64]byte{}
	benchPayload512  = [512]byte{}
	benchPayload1400 = [1400]byte{}
	benchPayload4000 = [4000]byte{}
)

func benchFragmentedOffer(b *testing.B, pub *aeron.Publication, payload []byte) {
	b.Helper()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		benchOfferSink = aeron.OfferRetryForTest(pub, payload)
	}
}

func setupBenchPublication(b *testing.B) *aeron.Publication {
	b.Helper()
	pub := aeron.NewTestPublication(b, aeron.DefaultTestPublicationLogConfig(), int64(1)<<62)
	benchOfferSink = pub.Offer(benchPayload64[:])
	return pub
}

func BenchmarkPublication_Offer_Small(b *testing.B) {
	pub := setupBenchPublication(b)
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		benchOfferSink = aeron.OfferRetryForTest(pub, benchPayload64[:])
	}
}

func BenchmarkPublication_Offer_Medium(b *testing.B) {
	pub := setupBenchPublication(b)
	b.ReportAllocs()
	b.Run("512", func(b *testing.B) {
		b.SetBytes(512)
		for b.Loop() {
			benchOfferSink = aeron.OfferRetryForTest(pub, benchPayload512[:])
		}
	})
	b.Run("fragmented/1400", func(b *testing.B) {
		benchFragmentedOffer(b, pub, benchPayload1400[:])
	})
	b.Run("fragmented/4000", func(b *testing.B) {
		benchFragmentedOffer(b, pub, benchPayload4000[:])
	})
}

func BenchmarkPublication_TryClaim_Commit(b *testing.B) {
	pub := setupBenchPublication(b)
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		termIndex, frameOffset, position, err := aeron.TryClaimRetryForTest(pub, 64)
		if err != nil {
			b.Fatal(err)
		}
		if position < 0 {
			b.Fatal("tryClaim failed")
		}
		termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
		termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, benchPayload64[:])
		if err := pub.Commit(termIndex, frameOffset); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublication_Offer_TermBoundary(b *testing.B) {
	cfg := aeron.DefaultTestPublicationLogConfig()
	cfg.RawTails[0] = logbuffer.PackRawTail(
		aeron.TestPublicationInitialTermID,
		int32(aeron.TestPublicationTermLength-128),
	)
	pub := aeron.NewTestPublication(b, cfg, int64(1)<<62)
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		benchOfferSink = aeron.OfferRetryForTest(pub, benchPayload64[:])
	}
}

func BenchmarkPublication_Offer_Parallel(b *testing.B) {
	pub := setupBenchPublication(b)
	b.ReportAllocs()
	b.SetBytes(64)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchOfferSink = aeron.OfferRetryForTest(pub, benchPayload64[:])
		}
	})
}

package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var (
	benchOfferSink   int64
	benchPayload64   = [64]byte{}
	benchPayload1400 = [1400]byte{}
	benchPayload4000 = [4000]byte{}
)

// benchOfferer is the offer surface benchOffer drives on either publication type.
type benchOfferer interface {
	OfferWithOffset(buffer []byte, offset, length int) int64
}

func benchOffer(b *testing.B, pub benchOfferer, payload []byte) {
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
	benchOffer(b, pub, benchPayload64[:])
}

func BenchmarkPublication_Offer_Fragmented(b *testing.B) {
	pub := setupBenchPublication(b)
	b.ReportAllocs()
	b.Run("1400", func(b *testing.B) {
		benchOffer(b, pub, benchPayload1400[:])
	})
	b.Run("4000", func(b *testing.B) {
		benchOffer(b, pub, benchPayload4000[:])
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
	benchOffer(b, pub, benchPayload64[:])
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

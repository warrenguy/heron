//go:build bench

package bench_test

import (
	"fmt"
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/test/bench/harness"
)

var publicationOfferSink int64

func newPublicationImagePair() (*aeron.Publication, *aeron.Image) {
	return aeron.NewInMemPublicationImagePairForBench(
		aeron.DefaultBenchPublicationConfig(),
		aeron.BenchPublicationLimitPosition,
	)
}

type publicationOfferStep func(
	b *testing.B,
	pub *aeron.Publication,
	payload []byte,
) int64

// Mirrors runExclusivePublicationOfferBench in exclusive_publication_bench_test.go;
// keep both in sync when the offer runner contract changes.
func runPublicationOfferBench(
	b *testing.B,
	payload []byte,
	offer publicationOfferStep,
) {
	b.Helper()

	pub := aeron.NewInMemPublicationForBench(
		aeron.DefaultBenchPublicationConfig(),
		aeron.BenchPublicationLimitPosition,
	)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for b.Loop() {
		pos := offer(b, pub, payload)
		if pos < 0 {
			continue
		}
		publicationOfferSink = pos
	}
}

func BenchmarkPublicationOffer(b *testing.B) {
	for _, size := range harness.PayloadSizes() {
		payload := harness.PayloadForSize(size)
		offer := harness.OfferWithTermOverflowReset
		if harness.IsFragmentedPayload(size) {
			offer = harness.OfferRetryWithTermOverflowReset
		}
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			runPublicationOfferBench(b, payload, offer)
		})
	}
	b.Run("admin-action-retry", func(b *testing.B) {
		runPublicationOfferBench(b, harness.Payload32[:], harness.OfferRetryWithTermOverflowReset)
	})
}

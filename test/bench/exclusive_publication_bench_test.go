//go:build bench

package bench_test

import (
	"fmt"
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/test/bench/harness"
)

var exclusivePublicationBenchSink int64

type exclusivePublicationBenchIter func(
	b *testing.B,
	ep *aeron.ExclusivePublication,
	payload []byte,
) (int64, bool)

func runExclusivePublicationBench(
	b *testing.B,
	payload []byte,
	iter exclusivePublicationBenchIter,
) {
	b.Helper()

	ep := aeron.NewInMemExclusivePublicationForBench(
		aeron.DefaultBenchExclusivePublicationConfig(),
		aeron.BenchPublicationLimitPosition,
	)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for b.Loop() {
		pos, ok := iter(b, ep, payload)
		if !ok {
			continue
		}
		exclusivePublicationBenchSink = pos
	}
}

type exclusivePublicationOfferStep func(
	b *testing.B,
	ep *aeron.ExclusivePublication,
	payload []byte,
) int64

func runExclusivePublicationOfferBench(
	b *testing.B,
	payload []byte,
	offer exclusivePublicationOfferStep,
) {
	runExclusivePublicationBench(b, payload, func(
		b *testing.B,
		ep *aeron.ExclusivePublication,
		payload []byte,
	) (int64, bool) {
		pos := offer(b, ep, payload)
		return pos, pos > 0
	})
}

func BenchmarkExclusivePublicationOffer(b *testing.B) {
	for _, size := range harness.ExclusiveOfferPayloadSizesForBench() {
		payload := harness.PayloadForSize(size)
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			runExclusivePublicationOfferBench(b, payload, harness.OfferExclusiveWithTermOverflowReset)
		})
	}
}

func BenchmarkExclusivePublicationTryClaimCommit(b *testing.B) {
	payload := harness.Payload32[:]
	runExclusivePublicationBench(b, payload, func(
		b *testing.B,
		ep *aeron.ExclusivePublication,
		payload []byte,
	) (int64, bool) {
		pos, err := harness.TryClaimCommitExclusiveWithTermOverflowReset(b, ep, payload)
		return pos, pos > 0 && err == nil
	})
}
